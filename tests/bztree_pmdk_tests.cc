// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <random>

#include "../bztree.h"
#include "util/performance_test.h"

#define TEST_POOL_NAME "pool_bztree" 
#define TEST_LAYOUT_NAME "layout_bztree"

class BzTreePMEMTest : public ::testing::Test {
 protected:
  bztree::BzTree *tree;

  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(TEST_POOL_NAME, TEST_LAYOUT_NAME, 1024 * 1024 * 1024),
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
  }

  void TearDown() override {
    pmwcas::Thread::ClearRegistry(true);
  }
};

TEST_F(BzTreePMEMTest, InsertTest) {
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto bztree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  pmwcas::DescriptorPool *pool = nullptr;
  pmdk_allocator->Allocate((void **) &pool, sizeof(pmwcas::DescriptorPool));

  new(pool) pmwcas::DescriptorPool(100000, 1, false);
  bztree::BzTree::ParameterSet param(3072, 0, 4096);
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

  pmwcas::DescriptorPool *pool;
  pmdk_allocator->Allocate((void **) &pool, sizeof(pmwcas::DescriptorPool));
  new(pool) pmwcas::DescriptorPool(2000, 1, false);

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

static uint32_t pool_size = 50000;
static uint32_t item_per_thread = 1000;
static uint32_t thread_count = 20;
GTEST_TEST(BztreePMEMTest, MiltiInsertTest) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(TEST_POOL_NAME,
                                                    TEST_LAYOUT_NAME,
                                                    1024 * 1024 * 1024),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(
      pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto bztree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  pmwcas::DescriptorPool *pool;
  pmdk_allocator->Allocate((void **) &pool, sizeof(pmwcas::DescriptorPool));
  new(pool) pmwcas::DescriptorPool(pool_size, thread_count, false);

  bztree::BzTree::ParameterSet param(3072, 0, 4096);
  new(bztree)bztree::BzTree(param, pool, reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));

  MultiThreadUpsertTest t(item_per_thread, thread_count, bztree);
  t.Run(thread_count);
  t.SanityCheck();
  pmwcas::Thread::ClearRegistry(true);
}

GTEST_TEST(BztreePMEMTest, MultiThreadReadback) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(TEST_POOL_NAME, TEST_LAYOUT_NAME, 1024 * 1024 * 1024),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto tree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  tree->Recovery();

  MultiThreadUpsertTest t(item_per_thread, thread_count, tree);
  t.SanityCheck();
  pmwcas::Thread::ClearRegistry(true);
}

GTEST_TEST(BztreePMEMTest, TreeSanityCheck) {
  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(TEST_POOL_NAME, TEST_LAYOUT_NAME, 1024 * 1024 * 1024),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto tree = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  tree->Recovery();
  tree->Dump();
  pmwcas::Thread::ClearRegistry(true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
