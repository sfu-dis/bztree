// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "util/performance_test.h"
#include "../bztree.h"

uint32_t descriptor_pool_size = 1000000;
uint32_t thread_count = 10;

struct MultiThreadRead : public pmwcas::PerformanceTest {
  bztree::BzTree *tree;
  uint32_t read_count;

  explicit MultiThreadRead(uint32_t read_count, bztree::BzTree *tree)
      : PerformanceTest() {
    this->read_count = read_count;
    this->tree = tree;
    InsertDummy();
  }

  void InsertDummy() {
    for (uint64_t i = 0; i < read_count; i += 1) {
      std::string key = std::to_string(i);
      tree->Insert(key.c_str(), static_cast<uint16_t>(key.length()), i);
    }
  }

  void Entry(size_t thread_index) override {
    WaitForStart();
    uint64_t payload;
    for (uint32_t i = 0; i < read_count; i++) {
      auto key = std::to_string(i);
      ASSERT_TRUE(tree->Read(key.c_str(), key.length(), &payload).IsOk());
      ASSERT_EQ(payload, i);
    }
  }
};

GTEST_TEST(BztreeTest, MultiThreadRead) {
  auto thread_count = pmwcas::Environment::Get()->GetCoreCount();
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count, nullptr)
  );
  bztree::BzTree::ParameterSet param(256, 128);
  std::unique_ptr<bztree::BzTree> tree(new bztree::BzTree(param, pool.get()));
  MultiThreadRead t(1000, tree.get());
  t.Run(thread_count);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                      pmwcas::TlsAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  return RUN_ALL_TESTS();
}