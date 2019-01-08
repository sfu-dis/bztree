// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gperftools/profiler.h>

#include "util/performance_test.h"
#include "../bztree.h"

uint32_t descriptor_pool_size = 1000000;

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

struct MultiThreadInsertTest : public pmwcas::PerformanceTest {
  bztree::BzTree *tree;
  uint32_t item_per_thread;
  uint32_t thread_count;
  explicit MultiThreadInsertTest(uint32_t item_per_thread, uint32_t thread_count, bztree::BzTree *tree)
      : tree(tree), thread_count(thread_count), item_per_thread(item_per_thread) {}

  void SanityCheck() {
    for (uint32_t i = 0; i < (thread_count + 1) * item_per_thread; i++) {
      auto i_str = std::to_string(i);
      uint64_t payload;
      auto rc = tree->Read(i_str.c_str(), static_cast<uint16_t>(i_str.length()), &payload);
      ASSERT_TRUE(rc.IsOk());
      ASSERT_EQ(payload, i);
    }
  }

  void Entry(size_t thread_index) override {
    WaitForStart();
    for (uint32_t i = 0; i < item_per_thread * 2; i++) {
      auto value = i + item_per_thread * thread_index;
      auto str_value = std::to_string(value);
      auto rc = tree->Insert(str_value.c_str(),
                             static_cast<uint16_t>(str_value.length()), value);
      ASSERT_TRUE(rc.IsOk() || rc.IsKeyExists());
    }
  }
};

GTEST_TEST(BztreeTest, MultiThreadRead) {
//  auto thread_count = pmwcas::Environment::Get()->GetCoreCount();
  uint32_t thread_count = 30;
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, 5, nullptr)
  );
  bztree::BzTree::ParameterSet param;
  std::unique_ptr<bztree::BzTree> tree(new bztree::BzTree(param, pool.get()));
  MultiThreadRead t(10000, tree.get());
  t.Run(thread_count);
  pmwcas::Thread::ClearRegistry();
}

GTEST_TEST(BztreeTest, MultiThreadInsertTest) {
  uint32_t thread_count = 40;
  uint32_t item_per_thread = 100;
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count, nullptr)
  );
  const auto kb = 1024;
  bztree::BzTree::ParameterSet param(kb * kb, 0, kb * kb);
  std::unique_ptr<bztree::BzTree> tree(new bztree::BzTree(param, pool.get()));

  MultiThreadInsertTest t(item_per_thread, thread_count, tree.get());
  t.Run(thread_count);
  pmwcas::Thread::ClearRegistry();
  t.SanityCheck();
}

GTEST_TEST(BztreeTest, MultiThreadInsertSplitTest) {
  uint32_t thread_count = 50;
  uint32_t item_per_thread = 10;
  std::unique_ptr<pmwcas::DescriptorPool> pool(
      new pmwcas::DescriptorPool(descriptor_pool_size, thread_count, nullptr)
  );
  uint32_t kb = 1024;
  bztree::BzTree::ParameterSet param(kb * kb, 0, kb * kb);
  std::unique_ptr<bztree::BzTree> tree(new bztree::BzTree(param, pool.get()));
  MultiThreadInsertTest t(item_per_thread, thread_count, tree.get());
  t.Run(thread_count);
  pmwcas::Thread::ClearRegistry();
  t.SanityCheck();
  tree->Dump();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                      pmwcas::TlsAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  return RUN_ALL_TESTS();
}