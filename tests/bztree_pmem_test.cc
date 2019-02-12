// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <random>

#include "../bztree.h"
#include "util/performance_test.h"

class BzTreePMEMTest : public ::testing::Test {
 protected:
  bztree::BzTree *tree;

  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create,
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
  }

  void TearDown() override {
    pmwcas::Thread::ClearRegistry();
  }
};

TEST_F(BzTreePMEMTest, InsertTest) {
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto bztree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  auto pool = reinterpret_cast<pmwcas::DescriptorPool *>(
      pmdk_allocator->Allocate(sizeof(pmwcas::DescriptorPool)));

  new(pool) pmwcas::DescriptorPool(100000, 1, nullptr, false);
  bztree::BzTree::ParameterSet param(256, 0, 256);
  new(bztree)bztree::BzTree(param, pool);
  pmdk_allocator->PersistPtr(bztree, sizeof(bztree::BzTree));
  pmdk_allocator->PersistPtr(pool, sizeof(pmwcas::DescriptorPool));

  tree = bztree;

  static const uint32_t kMaxKey = 20000;
  for (uint32_t i = 1; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    auto rc = bztree->Insert(key.c_str(), key.length(), i + 2000);
    ASSERT_TRUE(rc.IsOk());

    uint64_t payload = 0;
    rc = bztree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }
}

TEST_F(BzTreePMEMTest, ReadTest) {
  // Read everything back
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto pool = reinterpret_cast<pmwcas::DescriptorPool *>(
      pmdk_allocator->Allocate(sizeof(pmwcas::DescriptorPool)));
  new(pool) pmwcas::DescriptorPool(2000, 1, nullptr, false);

  auto root_obj = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));

  tree = root_obj;
  tree->SetPMWCASPool(pool);

  const uint32_t kMaxKey = 20000;
  for (uint32_t i = 1; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    uint64_t payload = 0;
    auto rc = tree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }
}
struct MultiThreadUpsertTest : public pmwcas::PerformanceTest {
  bztree::BzTree *tree;
  uint32_t item_per_thread;
  uint32_t thread_count;
  MultiThreadUpsertTest(uint32_t item_per_thread, uint32_t thread_count, bztree::BzTree *tree)
      : tree(tree), thread_count(thread_count), item_per_thread(item_per_thread) {}

  void SanityCheck() {
    for (uint32_t i = 0; i < (thread_count) * item_per_thread; i += 1) {
      auto i_str = std::to_string(i);
      uint64_t payload;
      auto rc = tree->Read(i_str.c_str(), static_cast<uint16_t>(i_str.length()), &payload);
      ASSERT_TRUE(rc.IsOk());
      ASSERT_TRUE(payload == i || payload == (i + 1));
    }
  }

  void Entry(size_t thread_index) override {
    WaitForStart();
    for (uint32_t i = 0; i < item_per_thread; i++) {
      auto value = i + item_per_thread * thread_index;
      auto str_value = std::to_string(value);
      auto rc = tree->Insert(str_value.c_str(),
                             static_cast<uint16_t>(str_value.length()), value);
      ASSERT_TRUE(rc.IsOk() || rc.IsKeyExists());
    }
  }
};

GTEST_TEST(BztreePMEMTest, MiltiInsertTest) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create,
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  uint32_t thread_count = 20;
  uint32_t item_per_thread = 1000;
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(
      pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto bztree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  auto pool = reinterpret_cast<pmwcas::DescriptorPool *>(
      pmdk_allocator->Allocate(sizeof(pmwcas::DescriptorPool)));

  new(pool) pmwcas::DescriptorPool(10000, thread_count, nullptr, false);
  bztree::BzTree::ParameterSet param;
  new(bztree)bztree::BzTree(param, pool, reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));

  MultiThreadUpsertTest t(item_per_thread, thread_count, bztree);
  t.Run(thread_count);
  pmwcas::Thread::ClearRegistry();
  t.SanityCheck();
}

GTEST_TEST(BztreePMEMTest, MultiThreadReadback) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create,
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  uint32_t thread_count = 20;
  uint32_t item_per_thread = 1000;
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto root_obj = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  auto pool = root_obj->GetPMWCASPool();
  new(pool) pmwcas::DescriptorPool(10000,
                                   thread_count,
                                   pool->GetDescriptor(),
                                   false);

  auto tree = root_obj;
  tree->SetPMWCASPool(pool);

  MultiThreadUpsertTest t(item_per_thread, thread_count, tree);
  t.SanityCheck();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}