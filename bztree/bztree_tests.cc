#include <glog/logging.h>
#include <gtest/gtest.h>

#include "bztree.h"

class LeafNodeFixtures : public ::testing::Test {
 public:
  void empty_node() {
    delete node;
    node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
    memset(node, 0, bztree::LeafNode::kNodeSize);
    new(node) bztree::LeafNode;
  }
  void insert_dummy() {
    node->Insert(0, "sorted_0", 8, 100, pool);
    node->Insert(0, "sorted_1", 8, 101, pool);
    node->Insert(0, "sorted_22", 9, 102, pool);
    node->Insert(0, "sorted_333", 10, 103, pool);
    node->Insert(0, "sorted_4444", 11, 104, pool);
    node->Insert(0, "unsorted_d", 10, 900, pool);
    node->Insert(0, "unsorted_b", 10, 901, pool);
    node->Insert(0, "unsorted_c", 10, 902, pool);
    node->Insert(0, "unsorted_a", 10, 903, pool);
  }
 protected:
  pmwcas::DescriptorPool *pool;
  bztree::LeafNode *node;
  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                        pmwcas::TlsAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
    pool = (pmwcas::DescriptorPool *) pmwcas::Allocator::Get()->Allocate(sizeof(pmwcas::DescriptorPool));
    new(pool) pmwcas::DescriptorPool(1000, 1, nullptr, false);

    node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
    memset(node, 0, bztree::LeafNode::kNodeSize);
    new(node) bztree::LeafNode;
  }

  void TearDown() override {
    delete node;
  }
};
TEST_F(LeafNodeFixtures, read_test) {
  pool->GetEpoch()->Protect();

  insert_dummy();
  ASSERT_EQ(node->Read("sorted_0", 8), 100);
  ASSERT_EQ(node->Read("sorted_22", 9), 102);
  ASSERT_EQ(node->Read("sorted_4444", 11), 104);

  ASSERT_EQ(node->Read("unsorted_a", 10), 903);
  ASSERT_EQ(node->Read("unsorted_b", 10), 901);
  ASSERT_EQ(node->Read("unsorted_c", 10), 902);

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, Insert) {
  pool->GetEpoch()->Protect();

  ASSERT_TRUE(node->Insert(0, "def", 3, 100, pool));
  ASSERT_TRUE(node->Insert(0, "bdef", 4, 101, pool));
  ASSERT_TRUE(node->Insert(0, "abc", 3, 102, pool));
  ASSERT_EQ(node->Read("def", 3), 100);
  ASSERT_EQ(node->Read("abc", 3), 102);

  node->Dump();

  auto *new_node = node->Consolidate(pool);
  new_node->Dump();
  ASSERT_TRUE(new_node->Insert(0, "apple", 5, 106, pool));
  ASSERT_EQ(new_node->Read("bdef", 4), 101);
  ASSERT_EQ(new_node->Read("apple", 5), 106);

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, duplicate_insert) {
  pool->GetEpoch()->Protect();
  insert_dummy();
  ASSERT_FALSE(node->Insert(0, "sorted_0", 8, 200, pool));
  ASSERT_TRUE(node->Insert(0, "abc", 3, 100, pool));

  ASSERT_EQ(node->Read("sorted_0", 8), 100);
  ASSERT_EQ(node->Read("abc", 3), 100);

  pool->GetEpoch()->Unprotect();
}

TEST(LeafNode, delete_test) {

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
