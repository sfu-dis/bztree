// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "../bztree.h"

const uint32_t node_size = 4096;

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

  new(pool) pmwcas::DescriptorPool(2000, 1, nullptr, false);
  bztree::BzTree::ParameterSet param;
  new(bztree)bztree::BzTree(param, pool);
  pmdk_allocator->PersistPtr(bztree, sizeof(bztree::BzTree));
  pmdk_allocator->PersistPtr(pool, sizeof(pmwcas::DescriptorPool));

  tree = bztree;

  static const uint32_t kMaxKey = 50;
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

  const uint32_t kMaxKey = 50;
  for (uint32_t i = 1; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    uint64_t payload = 0;
    auto rc = tree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}