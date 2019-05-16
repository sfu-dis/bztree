// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include <vector>
#include <memory>
#include <optional>

#include <pmwcas.h>
#include <mwcas/mwcas.h>

#include "util.h"
#include "basenode.h"
#include "nodes.h"

namespace bztree {

class Iterator;

class BzTree {
 public:
  // init a new tree
  explicit BzTree(const ParameterSet &param, nv_ptr<pmwcas::DescriptorPool> pool);

#ifdef PMEM
  void Recovery();
#endif

  void Dump();

  static BzTree *New(const ParameterSet &param, nv_ptr<pmwcas::DescriptorPool> pool);

  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload);
  ReturnCode Update(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Upsert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Delete(const char *key, uint16_t key_size);

  inline std::unique_ptr<Iterator> RangeScanBySize(const char *key1, uint16_t size1,
                                                   uint32_t scan_size) {
    return std::make_unique<Iterator>(this, key1, size1, scan_size);
  }

  LeafNode *TraverseToLeaf(Stack *stack, const char *key,
                           uint16_t key_size,
                           bool le_child = true);

  BaseNode *TraverseToNode(Stack *stack,
                           const char *key, uint16_t key_size,
                           BaseNode *stop_at = nullptr,
                           bool le_child = true);

  bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr, pmwcas::Descriptor *pd);

  inline nv_ptr<BaseNode> GetRootNode();

  ParameterSet parameters;

  nv_ptr<pmwcas::DescriptorPool> pmwcas_pool;

 private:
  nv_ptr<BaseNode> root;

  // epoch of this tree
  uint64_t index_epoch;
};

class Iterator {
 public:
  explicit Iterator(BzTree *tree, const char *begin_key, uint16_t begin_size, uint32_t scan_size) :
      key(begin_key), size(begin_size), tree(tree), remaining_size(scan_size) {
    node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
    node->RangeScanBySize(begin_key, begin_size, scan_size, &item_vec, tree->pmwcas_pool);
  }

  ~Iterator() = default;

  inline std::unique_ptr<Record> GetNext() {
    if (item_vec.empty() || remaining_size == 0) {
      return nullptr;
    }

    remaining_size -= 1;
    // we have more than one record
    if (item_vec.size() > 1) {
      auto front = std::move(item_vec.front());
      item_vec.pop_front();
      return front;
    }

    // there's only one record in the vector
    auto last_record = std::move(item_vec.front());
    item_vec.pop_front();

    node = this->tree->TraverseToLeaf(nullptr,
                                      last_record->GetKey(),
                                      last_record->meta.GetKeyLength(),
                                      false);
    if (node == nullptr) {
      return nullptr;
    }
    item_vec.clear();
    const char *last_key = last_record->GetKey();
    uint32_t last_len = last_record->meta.GetKeyLength();
    node->RangeScanBySize(last_key, last_len, remaining_size, &item_vec, tree->pmwcas_pool);

    // FIXME(hao): this a temp workaround
    // should fix traverse to leaf instead
    // check if we hit the same record
    if (!item_vec.empty()) {
      auto new_front = item_vec.front().get();
      if (KeyCompare(new_front->GetKey(), new_front->meta.GetKeyLength(),
                     last_record->GetKey(), last_record->meta.GetKeyLength()) == 0) {
        item_vec.clear();
        return last_record;
      }
    }
    return last_record;
  }

 private:
  const char *key;
  uint16_t size;
  uint32_t remaining_size;
  BzTree *tree;
  LeafNode *node;
  std::list<std::unique_ptr<Record>> item_vec;
};

}  // namespace bztree
