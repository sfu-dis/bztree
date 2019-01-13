// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "../bztree.h"

class LeafNodeFixtures : public ::testing::Test {
 public:
  void EmptyNode() {
    delete node;
    node = (bztree::LeafNode *) malloc(node->GetHeader()->size);
    memset(node, 0, node->GetHeader()->size);
    new(node) bztree::LeafNode;
  }

  // Dummy value:
  // sorted -> 0:10:100
  // unsorted -> 200:10:300
  void InsertDummy() {
    for (uint32_t i = 0; i < 100; i += 10) {
      auto str = std::to_string(i);
      node->Insert(str.c_str(), (uint16_t) str.length(), i, pool);
    }
    auto *new_node = node->Consolidate(pool);
    for (uint32_t i = 200; i < 300; i += 10) {
      auto str = std::to_string(i);
      new_node->Insert(str.c_str(), (uint16_t) str.length(), i, pool);
    }
    delete node;
    node = new_node;
  }

  void ASSERT_READ(bztree::LeafNode *node, const char *key, uint16_t key_size, uint64_t expected) {
    uint64_t payload;
    node->Read(key, key_size, &payload, pool);
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
    const uint32_t node_size = 4096;
    pool = new pmwcas::DescriptorPool(1000, 1, nullptr, false);
    node = (bztree::LeafNode *) malloc(node_size);
    memset(node, 0, node_size);
    new(node) bztree::LeafNode;
  }

  void TearDown() override {
    delete node;
    delete pool;
    pmwcas::Thread::ClearRegistry();
  }
};

TEST_F(LeafNodeFixtures, Read) {
  pmwcas::EpochGuard guard(pool->GetEpoch());
  InsertDummy();
  uint64_t payload;
  ASSERT_READ(node, "0", 1, 0);
  ASSERT_READ(node, "10", 2, 10);
  ASSERT_TRUE(node->Read("100", 3, &payload, pool).IsNotFound());

  ASSERT_READ(node, "200", 3, 200);
  ASSERT_READ(node, "210", 3, 210);
  ASSERT_READ(node, "280", 3, 280);
}

TEST_F(LeafNodeFixtures, Insert) {
  pmwcas::EpochGuard guard(pool->GetEpoch());

  ASSERT_TRUE(node->Insert("def", 3, 100, pool).IsOk());
  ASSERT_TRUE(node->Insert("bdef", 4, 101, pool).IsOk());
  ASSERT_TRUE(node->Insert("abc", 3, 102, pool).IsOk());
  ASSERT_READ(node, "def", 3, 100);
  ASSERT_READ(node, "abc", 3, 102);

  auto *new_node = node->Consolidate(pool);
  ASSERT_TRUE(new_node->Insert("apple", 5, 106, pool).IsOk());
  ASSERT_READ(new_node, "bdef", 4, 101);
  ASSERT_READ(new_node, "apple", 5, 106);
}

TEST_F(LeafNodeFixtures, DuplicateInsert) {
  pmwcas::EpochGuard guard(pool->GetEpoch());
  InsertDummy();
  ASSERT_TRUE(node->Insert("10", 2, 111, pool).IsKeyExists());
  ASSERT_TRUE(node->Insert("11", 2, 1212, pool).IsOk());

  ASSERT_READ(node, "10", 2, 10);
  ASSERT_READ(node, "11", 2, 1212);

  auto *new_node = node->Consolidate(pool);

  ASSERT_TRUE(new_node->Insert("11", 2, 1213, pool).IsKeyExists());
  ASSERT_READ(new_node, "11", 2, 1212);

  ASSERT_TRUE(new_node->Insert("201", 3, 201, pool).IsOk());
  ASSERT_READ(new_node, "201", 3, 201);
}

TEST_F(LeafNodeFixtures, Delete) {
  pmwcas::EpochGuard guard(pool->GetEpoch());
  InsertDummy();
  uint64_t payload;
  ASSERT_READ(node, "40", 2, 40);
  ASSERT_TRUE(node->Delete("40", 2, pool).IsOk());
  ASSERT_TRUE(node->Read("40", 2, &payload, pool).IsNotFound());

  auto new_node = node->Consolidate(pool);

  ASSERT_READ(new_node, "200", 3, 200);
  ASSERT_TRUE(new_node->Delete("200", 3, pool).IsOk());
  ASSERT_TRUE(new_node->Read("200", 3, &payload, pool).IsNotFound());
}

TEST_F(LeafNodeFixtures, SplitPrep) {
  pmwcas::EpochGuard guard(pool->GetEpoch());
  InsertDummy();

  ASSERT_TRUE(node->Insert("abc", 3, 100, pool).IsOk());
  ASSERT_TRUE(node->Insert("bdef", 4, 101, pool).IsOk());
  ASSERT_TRUE(node->Insert("abcd", 4, 102, pool).IsOk());
  ASSERT_TRUE(node->Insert("deadbeef", 8, 103, pool).IsOk());
  ASSERT_TRUE(node->Insert("parker", 6, 104, pool).IsOk());
  ASSERT_TRUE(node->Insert("deadpork", 8, 105, pool).IsOk());
  ASSERT_TRUE(node->Insert("toronto", 7, 106, pool).IsOk());

  bztree::Stack stack;
  bztree::LeafNode *left = nullptr;
  bztree::LeafNode *right = nullptr;
  bztree::InternalNode *parent = node->PrepareForSplit(stack, 3000, pool, &left, &right);
  ASSERT_NE(parent, nullptr);
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
}
TEST_F(LeafNodeFixtures, Update) {
  pmwcas::EpochGuard guard(pool->GetEpoch());
  InsertDummy();
  ASSERT_READ(node, "10", 2, 10);
  ASSERT_TRUE(node->Update("10", 2, 11, pool).IsOk());
  ASSERT_READ(node, "10", 2, 11);

  ASSERT_READ(node, "200", 3, 200);
  ASSERT_TRUE(node->Update("200", 3, 201, pool).IsOk());
  ASSERT_READ(node, "200", 3, 201);
}

TEST_F(LeafNodeFixtures, RangeScan) {
  pool->GetEpoch()->Protect();
  InsertDummy();
  std::vector<std::unique_ptr<bztree::Record>> result;
  ASSERT_TRUE(node->RangeScan("10", 2, "40", 2, &result, pool).IsOk());
  ASSERT_EQ(result.size(), 14);
  ASSERT_EQ(result[0]->GetPayload(), 10);
  ASSERT_EQ(result[2]->GetPayload(), 200);
  ASSERT_EQ(result[13]->GetPayload(), 40);
  ASSERT_EQ(std::string(result[0]->GetKey(), result[0]->meta.GetKeyLength()),
            std::string("10"));
  ASSERT_EQ(std::string(result[2]->GetKey(), result[2]->meta.GetKeyLength()),
            std::string("200"));
  ASSERT_EQ(std::string(result[13]->GetKey(), result[13]->meta.GetKeyLength()),
            std::string("40"));
  pool->GetEpoch()->Unprotect();
}

class BzTreeTest : public ::testing::Test {
 protected:
  pmwcas::DescriptorPool *pool;
  bztree::BzTree *tree;

//  Insert 0:10:100
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
    pool = new pmwcas::DescriptorPool(10000, 1, nullptr, false);
    bztree::BzTree::ParameterSet param(256, 128);
    tree = new bztree::BzTree(param, pool);
  }

  void TearDown() override {
    delete tree;
    delete pool;
    pmwcas::Thread::ClearRegistry();
  }
};

TEST_F(BzTreeTest, Insert) {
  static const uint32_t kMaxKey = 1400;
  for (uint32_t i = 100; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    auto rc = tree->Insert(key.c_str(), key.length(), i + 2000);
    ASSERT_TRUE(rc.IsOk());

    uint64_t payload = 0;
    rc = tree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }

  // Read everything back
  for (uint32_t i = 100; i < kMaxKey; ++i) {
    std::string key = std::to_string(i);
    uint64_t payload = 0;
    auto rc = tree->Read(key.c_str(), key.length(), &payload);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(payload == i + 2000);
  }
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

TEST_F(BzTreeTest, Upsert) {
  uint64_t payload;
  InsertDummy();
  ASSERT_TRUE(tree->Read("abc", 3, &payload).IsNotFound());
  ASSERT_TRUE(tree->Upsert("abc", 3, 42).IsOk());
  ASSERT_TRUE(tree->Read("abc", 3, &payload).IsOk());
  ASSERT_EQ(payload, 42);

  ASSERT_TRUE(tree->Upsert("20", 2, 21).IsOk());
  ASSERT_TRUE(tree->Read("20", 2, &payload).IsOk());
  ASSERT_EQ(payload, 21);
}

TEST_F(BzTreeTest, Delete) {
  InsertDummy();
  uint64_t payload;
  ASSERT_TRUE(tree->Delete("11", 2).IsNotFound());
  ASSERT_TRUE(tree->Delete("10", 2).IsOk());
  ASSERT_TRUE(tree->Read("10", 2, &payload).IsNotFound());
}

TEST_F(BzTreeTest, RangeScan) {
  static const uint32_t kMaxKey = 125;
  for (uint32_t i = 100; i <= kMaxKey; i++) {
    auto key = std::to_string(i);
    tree->Insert(key.c_str(), static_cast<uint16_t>(key.length()), i);
  }
  auto iter = tree->RangeScan("100", 3, "125", 3);
  for (uint32_t i = 100; i <= 125; i += 1) {
    auto record = iter->GetNext();
    ASSERT_TRUE(record != nullptr);
    auto key = std::string(record->GetKey(), 3);
    ASSERT_EQ(i, record->GetPayload());
    ASSERT_EQ(key, std::to_string(i));
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
