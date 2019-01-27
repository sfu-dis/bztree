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
  const char *leaf_pool = "leaf_pool";
  const char *layout = "leaf_node";
  auto pool = new pmwcas::DescriptorPool(1000, 1, nullptr);

  // creating
  auto allocator = Allocator::New(leaf_pool, layout);
  auto root = allocator->GetRoot(sizeof(bztree::LeafNode));
  auto raw_root = pmemobj_direct(root);
  new(raw_root)bztree::LeafNode;
  pmemobj_persist(allocator->GetPool(), raw_root, sizeof(bztree::LeafNode));

  // inserting
  allocator.reset();
  allocator = Allocator::New(leaf_pool, layout);
  root = allocator->GetRoot(sizeof(bztree::LeafNode));
  auto node = reinterpret_cast<bztree::LeafNode *>(pmemobj_direct(root));

  pmwcas::EpochGuard guard(pool->GetEpoch());
  for (uint32_t i = 0; i < 100; i += 1) {
    auto str = std::to_string(i);
    node->Insert(str.c_str(), (uint16_t) str.length(), i, pool, node_size);
  }

  // read back
  allocator.reset();
  allocator = Allocator::New(leaf_pool, layout);
  root = allocator->GetRoot(sizeof(bztree::LeafNode));
  node = reinterpret_cast<bztree::LeafNode *>(pmemobj_direct(root));
  for (uint32_t i = 0; i < 100; i += 1) {
    auto str = std::to_string(i);
    uint64_t tmp_payload;
    node->Read(str.c_str(), str.length(), &tmp_payload, pool);
    ASSERT_EQ(tmp_payload, i);
  }
  remove(leaf_pool);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                      pmwcas::TlsAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  return RUN_ALL_TESTS();
}