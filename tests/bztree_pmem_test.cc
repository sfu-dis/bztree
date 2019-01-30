// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "../allocator.h"
#include "../bztree.h"

const uint32_t node_size = 4096;

TEST(LeafNodePmemTest, ReadWriteTest) {
  const char *pool_name = "leaf_pool";
  const char *layout_name = "leaf_node";
  auto pool = new pmwcas::DescriptorPool(1000, 1, nullptr);

  // creating
  auto allocator = Allocator::New(pool_name, layout_name);
  auto raw_root = allocator->GetDirectRoot(sizeof(bztree::LeafNode));
  new(raw_root)bztree::LeafNode;
  allocator->PersistPtr(raw_root, sizeof(bztree::LeafNode));

  // inserting
  allocator.reset();
  allocator = Allocator::New(pool_name, layout_name);
  auto node = reinterpret_cast<bztree::LeafNode *>(
      allocator->GetDirectRoot(sizeof(bztree::LeafNode)));

  pmwcas::EpochGuard guard(pool->GetEpoch());
  for (uint32_t i = 0; i < 100; i += 1) {
    auto str = std::to_string(i);
    node->Insert(str.c_str(), (uint16_t) str.length(), i, pool, node_size);
  }

  // read back
  allocator.reset();
  allocator = Allocator::New(pool_name, layout_name);
  node = reinterpret_cast<bztree::LeafNode *>(
      allocator->GetDirectRoot(sizeof(bztree::LeafNode)));
  for (uint32_t i = 0; i < 100; i += 1) {
    auto str = std::to_string(i);
    uint64_t tmp_payload;
    node->Read(str.c_str(), str.length(), &tmp_payload, pool);
    ASSERT_EQ(tmp_payload, i);
  }
  remove(pool_name);
}

TEST(BzTreePmemTest, LeafOnlyTest) {
  const char *pool_name = "bztree_pool";
  const char *layout_name = "bztree_layout";
  auto pool = std::make_unique<pmwcas::DescriptorPool>(1000, 1, nullptr);

  // set root
  auto allocator = Allocator::New(pool_name, layout_name);
  auto root_tree = reinterpret_cast<bztree::BzTree *>(
      allocator->GetDirectRoot(sizeof(bztree::BzTree)));

  bztree::BzTree::ParameterSet param;
  auto leaf_node = reinterpret_cast<bztree::LeafNode *>(
      allocator->AllocDirect(sizeof(param.leaf_node_size), true));
  new(leaf_node)bztree::LeafNode();
  new(root_tree)bztree::BzTree(param, pool.get(), leaf_node);
  allocator->PersistPtr(root_tree, sizeof(bztree::BzTree));

  // insert
  allocator.reset();
  allocator = Allocator::New(pool_name, layout_name);
  auto insert_tree = reinterpret_cast<bztree::BzTree *>(
      allocator->GetDirectRoot(sizeof(bztree::BzTree)));
  for (uint32_t i = 0; i < 64; i++) {
    std::string str = std::to_string(i);
    insert_tree->Insert(str.c_str(), str.length(), i);
  }

  // read back
  allocator.reset();
  allocator = Allocator::New(pool_name, layout_name);
  auto read_tree = reinterpret_cast<bztree::BzTree *>(
      allocator->GetDirectRoot(sizeof(bztree::BzTree))
  );
  for (uint32_t i = 0; i < 64; i++) {
    std::string str = std::to_string(i);
    uint64_t payload;
    read_tree->Read(str.c_str(), str.length(), &payload);
    ASSERT_EQ(payload, i);
  }
}
class BzTreePMEMTest : public ::testing::Test {
 protected:
  pmwcas::DescriptorPool *pool;
  bztree::BzTree *tree;

  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create,
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
    pool = new pmwcas::DescriptorPool(2000, 1, nullptr, false);
  }

  void TearDown() override {
    delete pool;
    pmwcas::Thread::ClearRegistry();
    pmwcas::UninitLibrary();
  }
};

TEST_F(BzTreePMEMTest, InsertTest) {
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::BzTree::ParameterSet param(256, 128, 256);
  auto root = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  new(root)bztree::BzTree(param, pool);
  pmdk_allocator->PersistPtr(root, sizeof(bztree::BzTree));
  tree = root;

  static const uint32_t kMaxKey = 50;
  for (uint32_t i = 1; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    auto rc = tree->Insert(key.c_str(), key.length(), i + 2000);
    ASSERT_TRUE(rc.IsOk());

    uint64_t payload = 0;
    rc = tree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }
}

TEST_F(BzTreePMEMTest, ReadTest) {
  // Read everything back
  auto pmdk_allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  auto root = reinterpret_cast<bztree::BzTree *>(pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  tree = root;

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