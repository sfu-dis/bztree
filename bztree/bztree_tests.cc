// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "bztree.h"

class LeafNodeFixtures : public ::testing::Test {
 public:
  void EmptyNode() {
    delete node;
    node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
    memset(node, 0, bztree::LeafNode::kNodeSize);
    new(node) bztree::LeafNode;
  }

  // Dummy value:
  // sorted -> 0:10:100
  // unsorted -> 200:10:300
  void InsertDummy() {
    for (uint32_t i = 0; i < 100; i += 10) {
      auto str = std::to_string(i);
      node->Insert(0, str.c_str(), (uint16_t) str.length(), i, pool);
    }
    auto *new_node = node->Consolidate(pool);
    for (uint32_t i = 200; i < 300; i += 10) {
      auto str = std::to_string(i);
      new_node->Insert(0, str.c_str(), (uint16_t) str.length(), i, pool);
    }
    delete node;
    node = new_node;
  }

  void ASSERT_READ(bztree::LeafNode *node, const char *key, uint16_t key_size, uint64_t expected) {
    uint64_t payload;
    node->Read(key, key_size, &payload);
    ASSERT_EQ(payload, expected);
  }

 protected:
  pmwcas::DescriptorPool *pool;
  bztree::LeafNode *node;
  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                        pmwcas::TlsAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
    pool = reinterpret_cast<pmwcas::DescriptorPool *>(
        pmwcas::Allocator::Get()->Allocate(sizeof(pmwcas::DescriptorPool)));
    new(pool) pmwcas::DescriptorPool(1000, 1, nullptr, false);

    node = (bztree::LeafNode *) malloc(bztree::LeafNode::kNodeSize);
    memset(node, 0, bztree::LeafNode::kNodeSize);
    new(node) bztree::LeafNode;
  }

  void TearDown() override {
    delete node;
  }
};

TEST_F(LeafNodeFixtures, Read) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  uint64_t payload;
  ASSERT_READ(node, "0", 1, 0);
  ASSERT_READ(node, "10", 2, 10);
  ASSERT_TRUE(node->Read("100", 3, &payload).IsNotFound());

  ASSERT_READ(node, "200", 3, 200);
  ASSERT_READ(node, "210", 3, 210);
  ASSERT_READ(node, "280", 3, 280);

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, Insert) {
  pool->GetEpoch()->Protect();

  ASSERT_TRUE(node->Insert(0, "def", 3, 100, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "bdef", 4, 101, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "abc", 3, 102, pool).IsOk());
  ASSERT_READ(node, "def", 3, 100);
  ASSERT_READ(node, "abc", 3, 102);

  node->Dump();

  auto *new_node = node->Consolidate(pool);
  new_node->Dump();
  ASSERT_TRUE(new_node->Insert(0, "apple", 5, 106, pool).IsOk());
  ASSERT_READ(new_node, "bdef", 4, 101);
  ASSERT_READ(new_node, "apple", 5, 106);

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, DuplicateInsert) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  ASSERT_TRUE(node->Insert(0, "10", 2, 111, pool).IsKeyExists());
  ASSERT_TRUE(node->Insert(0, "11", 2, 1212, pool).IsOk());

  ASSERT_READ(node, "10", 2, 10);
  ASSERT_READ(node, "11", 2, 1212);

  auto *new_node = node->Consolidate(pool);

  ASSERT_TRUE(new_node->Insert(0, "11", 2, 1213, pool).IsKeyExists());
  ASSERT_READ(new_node, "11", 2, 1212);

  ASSERT_TRUE(new_node->Insert(0, "201", 3, 201, pool).IsOk());
  ASSERT_READ(new_node, "201", 3, 201);

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, Delete) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  uint64_t payload;
  ASSERT_READ(node, "40", 2, 40);
  ASSERT_TRUE(node->Delete("40", 2, pool).IsOk());
  ASSERT_TRUE(node->Read("40", 2, &payload).IsNotFound());

  auto new_node = node->Consolidate(pool);

  ASSERT_READ(new_node, "200", 3, 200);
  ASSERT_TRUE(new_node->Delete("200", 3, pool).IsOk());
  ASSERT_TRUE(new_node->Read("200", 3, &payload).IsNotFound());

  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, SplitPrep) {
  pool->GetEpoch()->Protect();
  InsertDummy();

  ASSERT_TRUE(node->Insert(0, "abc", 3, 100, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "bdef", 4, 101, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "abcd", 4, 102, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "deadbeef", 8, 103, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "parker", 6, 104, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "deadpork", 8, 105, pool).IsOk());
  ASSERT_TRUE(node->Insert(0, "toronto", 7, 106, pool).IsOk());

  node->Dump();

  bztree::Stack stack;
  bztree::InternalNode *parent = nullptr;
  bztree::LeafNode *left = nullptr;
  bztree::LeafNode *right = nullptr;
  ASSERT_TRUE(node->PrepareForSplit(0, stack, &parent, &left, &right, pool).IsOk());

  left->Dump();
  right->Dump();
  parent->Dump();
  pool->GetEpoch()->Unprotect();
}
TEST_F(LeafNodeFixtures, Update) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  ASSERT_READ(node, "10", 2, 10);
  ASSERT_TRUE(node->Update(0, "10", 2, 11, pool).IsOk());
  ASSERT_READ(node, "10", 2, 11);

  ASSERT_READ(node, "200", 3, 200);
  ASSERT_TRUE(node->Update(0, "200", 3, 201, pool).IsOk());
  ASSERT_READ(node, "200", 3, 201);
  pool->GetEpoch()->Unprotect();
}

TEST_F(LeafNodeFixtures, Upsert) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  uint64_t payload;

  ASSERT_READ(node, "20", 2, 20);
  ASSERT_TRUE(node->Upsert(0, "20", 2, 21, pool).IsOk());
  ASSERT_READ(node, "20", 2, 21);

  ASSERT_READ(node, "210", 3, 210);
  ASSERT_TRUE(node->Upsert(0, "210", 3, 211, pool).IsOk());
  ASSERT_READ(node, "210", 3, 211);

//  Non-existing upsert
  ASSERT_TRUE(node->Read("21", 2, &payload).IsNotFound());
  ASSERT_TRUE(node->Upsert(0, "21", 2, 21, pool).IsOk());
  ASSERT_READ(node, "21", 2, 21);

  ASSERT_TRUE(node->Read("211", 3, &payload).IsNotFound());
  ASSERT_TRUE(node->Upsert(0, "211", 3, 211, pool).IsOk());
  ASSERT_READ(node, "211", 3, 211);

  pool->GetEpoch()->Unprotect();
}

class BzTreeTest : public ::testing::Test {
 protected:
  pmwcas::DescriptorPool *pool;
  bztree::BzTree *tree;

  void InsertDummy() {
    for (uint64_t i = 0; i < 100; i += 10) {
      std::string key = std::to_string(i);
      tree->Insert(key.c_str(), key.length(), i);
    }
  }

  void SetUp() override {
    pmwcas::InitLibrary(pmwcas::TlsAllocator::Create,
                        pmwcas::TlsAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
    pool = reinterpret_cast<pmwcas::DescriptorPool *>(
        pmwcas::Allocator::Get()->Allocate(sizeof(pmwcas::DescriptorPool)));
    new(pool) pmwcas::DescriptorPool(1000, 1, nullptr, false);

    bztree::BzTree::ParameterSet param(256);
    tree = new bztree::BzTree(param, pool);
  }

  void TearDown() override {
    delete tree;
  }
};

TEST_F(BzTreeTest, Insert) {
  for (uint32_t i = 100; i < 150; ++i) {
    std::string key = std::to_string(i);
    auto rc = tree->Insert(key.c_str(), key.length(), 127);
    ASSERT_TRUE(rc.IsOk());
//    tree->Dump();
  }
  tree->Dump();
}

TEST_F(BzTreeTest, Read) {
  uint64_t payload;

  ASSERT_TRUE(tree->Read("10", 2, &payload).IsNotFound());

  InsertDummy();
  ASSERT_TRUE(tree->Read("10", 2, &payload).IsOk());
  ASSERT_EQ(payload, 10);

  ASSERT_TRUE(tree->Read("11", 2, &payload).IsNotFound());
}

TEST_F(BzTreeTest, Update) {
  uint64_t payload;
  InsertDummy();

  tree->Read("20", 2, &payload);
  ASSERT_EQ(payload, 20);

  ASSERT_TRUE(tree->Update("20", 2, 21).IsOk());
  tree->Read("20", 2, &payload);
  ASSERT_EQ(payload, 21);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
