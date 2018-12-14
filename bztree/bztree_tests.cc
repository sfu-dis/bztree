#include <glog/logging.h>
#include <gtest/gtest.h>

#include "bztree.h"

TEST(LeafNode, Insert) {
  bztree::LeafNode *node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
  memset(node, 0, bztree::LeafNode::kNodeSize);
  new(node) bztree::LeafNode;
  node->Dump();

  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                      pmwcas::TlsAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);

  pmwcas::DescriptorPool *pool =
      (pmwcas::DescriptorPool *) pmwcas::Allocator::Get()->Allocate(sizeof(pmwcas::DescriptorPool));
  new(pool) pmwcas::DescriptorPool(100, 1, nullptr, false);

  pool->GetEpoch()->Protect();

  ASSERT_TRUE(node->Insert(0, "def", 3, 100, pool));
  ASSERT_TRUE(node->Insert(0, "bdef", 4, 101, pool));
  ASSERT_TRUE(node->Insert(0, "abc", 3, 102, pool));

  ASSERT_EQ(node->Read("def", 3), 100);
  ASSERT_EQ(node->Read("abc", 3), 102);

  node->Dump();

  auto *new_node = node->Consolidate(pool);
  new_node->Dump();
  ASSERT_EQ(new_node->Read("bdef", 4), 101);

  pool->GetEpoch()->Unprotect();
}

TEST(LeafNode, duplicate_insert) {
  auto *node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
  memset((void *) node, 0, bztree::LeafNode::kNodeSize);
  new(node) bztree::LeafNode();
  node->Dump();

  pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                      pmwcas::TlsAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto *pool = (pmwcas::DescriptorPool *) pmwcas::Allocator::Get()->Allocate(sizeof(pmwcas::DescriptorPool));
  new(pool) pmwcas::DescriptorPool(100, 1, nullptr, false);

  pool->GetEpoch()->Protect();

  ASSERT_TRUE(node->Insert(0, "abc", 3, 100, pool));
  ASSERT_TRUE(node->Insert(0, "bdef", 4, 101, pool));
  ASSERT_FALSE(node->Insert(0, "abc", 3, 102, pool));
  ASSERT_TRUE(node->Insert(0, "abcd", 4, 104, pool));

  ASSERT_EQ(node->Read("abc", 3), 100);
  ASSERT_EQ(node->Read("bdef", 4), 101);

  auto *new_node = node->Consolidate(pool);

  ASSERT_FALSE(new_node->Insert(0, "abcd", 4, 100, pool));
  ASSERT_TRUE(new_node->Insert(0, "aaa", 3, 105, pool));

  ASSERT_EQ(new_node->Read("aaa", 3), 105);
  ASSERT_EQ(new_node->Read("abcd", 4), 104);

  pool->GetEpoch()->Unprotect();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
