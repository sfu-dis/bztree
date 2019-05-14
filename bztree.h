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

#ifndef ALWAYS_ASSERT
#define ALWAYS_ASSERT(expr) (expr) ? (void)0 : abort()
#endif

template<typename T>
using nv_ptr= pmwcas::nv_ptr<T>;

namespace bztree {

struct Record {
  RecordMetadata meta;
  char data[0];

  explicit Record(RecordMetadata meta) : meta(meta) {}
  static inline Record *New(RecordMetadata meta, BaseNode *node) {
    if (!meta.IsVisible()) {
      return nullptr;
    }

    Record *r = reinterpret_cast<Record *>(malloc(meta.GetTotalLength() + sizeof(meta)));
    memset(r, 0, meta.GetTotalLength() + sizeof(Record));
    new(r) Record(meta);

    // Key will never be changed and it will not be a pmwcas descriptor
    // but payload is fixed length 8-byte value, can be updated by pmwcas
    memcpy(r->data, reinterpret_cast<char *>(node) + meta.GetOffset(), meta.GetPaddedKeyLength());

    auto source_addr = (reinterpret_cast<char *>(node) + meta.GetOffset());
    auto payload = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        source_addr + meta.GetPaddedKeyLength())->GetValueProtected();
    memcpy(r->data + meta.GetPaddedKeyLength(), &payload, sizeof(payload));
    return r;
  }

  inline const uint64_t GetPayload() {
    return *reinterpret_cast<uint64_t *>(data + meta.GetPaddedKeyLength());
  }
  inline const char *GetKey() const { return data; }
  inline bool operator<(const Record &out) {
    int cmp = KeyCompare(this->GetKey(), this->meta.GetKeyLength(),
                                   out.GetKey(), out.meta.GetKeyLength());
    return cmp < 0;
  }
};
class Iterator;
class BzTree {
 public:
  struct ParameterSet {
    const uint32_t split_threshold;
    const uint32_t merge_threshold;
    const uint32_t leaf_node_size;
    ParameterSet() : split_threshold(3072), merge_threshold(1024), leaf_node_size(4096) {}
    ParameterSet(uint32_t split_threshold, uint32_t merge_threshold, uint32_t leaf_node_size = 4096)
        : split_threshold(split_threshold),
          merge_threshold(merge_threshold),
          leaf_node_size(leaf_node_size) {}
    ~ParameterSet() {}
  };

  // init a new tree
  BzTree(const ParameterSet &param, nv_ptr<pmwcas::DescriptorPool> pool, uint64_t pmdk_addr = 0)
      : parameters(param),
        root(nullptr),
        pmdk_addr(pmdk_addr),
        index_epoch(0),
        pmwcas_pool(pool) {
    global_epoch = index_epoch;
    pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
    auto *pd = pool->AllocateDescriptor();
    auto index = pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(&root),
                                        reinterpret_cast<uint64_t>(nullptr),
                                        pmwcas::Descriptor::kRecycleOnRecovery);
    auto root_ptr = pd->GetNewValuePtr(index);
    LeafNode::New(reinterpret_cast<LeafNode **>(root_ptr), param.leaf_node_size);
    pd->MwCAS();
  }

#ifdef PMEM
  void Recovery() {
    index_epoch += 1;
    // avoid multiple increment if there are multiple bztrees
    if (global_epoch != index_epoch) {
      global_epoch = index_epoch;
    }
    pmwcas_pool->Recovery(false);

    pmwcas::NVRAM::Flush(sizeof(bztree::BzTree), this);
  }
#endif

  void Dump();

  inline static BzTree *New(const ParameterSet &param, nv_ptr<pmwcas::DescriptorPool> pool) {
    BzTree *tree;
    pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(&tree), sizeof(BzTree));
    new(tree) BzTree(param, pool);
    return tree;
  }

  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload);
  ReturnCode Update(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Upsert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Delete(const char *key, uint16_t key_size);

  inline std::unique_ptr<Iterator> RangeScanByKey(const char *key1, uint16_t size1,
                                                  const char *key2, uint16_t size2) {
    return std::make_unique<Iterator>(this, key1, size1, key2, size2);
  }
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

  inline uint64_t GetPMDKAddr() {
    return pmdk_addr;
  }

  inline uint64_t GetEpoch() {
    return index_epoch;
  }

  ParameterSet parameters;
  nv_ptr<pmwcas::DescriptorPool> pmwcas_pool;

  bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr, pmwcas::Descriptor *pd);
 private:
  BaseNode *root;
  uint64_t pmdk_addr;
  uint64_t index_epoch;

  inline BaseNode *GetRootNodeSafe() {
    auto root_node = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        &root)->GetValueProtected();
#ifdef PMDK
    return Allocator::Get()->GetDirect(reinterpret_cast<BaseNode *>(root_node));
#else
    return reinterpret_cast<BaseNode *>(root_node);
#endif
  }
};

class Iterator {
 public:
  explicit Iterator(BzTree *tree,
                    const char *begin_key,
                    uint16_t begin_size,
                    const char *end_key,
                    uint16_t end_size) {
    this->begin_key = begin_key;
    this->end_key = end_key;
    this->begin_size = begin_size;
    this->end_size = end_size;
    this->tree = tree;
    this->scan_size = ~uint32_t{0};
    node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
    node->RangeScanByKey(begin_key, begin_size, end_key, end_size, &item_vec, tree->pmwcas_pool);
    item_it = item_vec.begin();
    by_key = true;
  }

  explicit Iterator(BzTree *tree, const char *begin_key, uint16_t begin_size, uint32_t scan_size) {
    this->begin_key = begin_key;
    this->end_key = nullptr;
    this->begin_size = begin_size;
    this->end_size = 0;
    this->tree = tree;
    this->scan_size = scan_size;
    node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
    node->RangeScanBySize(begin_key, begin_size, &scan_size, &item_vec, tree->pmwcas_pool);
    item_it = item_vec.begin();
    by_key = false;
  }

  ~Iterator() {
    for (auto &v : item_vec) {
      free(v);  // malloc-allocated Record
    }
  }

  inline Record *GetNextByKey() {
    if (item_vec.size() == 0) {
      return nullptr;
    }

    auto old_it = item_it;
    if (item_it != item_vec.end()) {
      item_it += 1;
      return *old_it;
    } else {
      auto &last_record = item_vec.back();
      node = this->tree->TraverseToLeaf(nullptr,
                                        last_record->GetKey(),
                                        last_record->meta.GetKeyLength(),
                                        false);
      if (node) {
        item_vec.clear();
        node->RangeScanByKey(last_record->GetKey(),
                             last_record->meta.GetKeyLength(),
                             end_key,
                             end_size,
                             &item_vec,
                             tree->pmwcas_pool);
        item_it = item_vec.begin();
        return GetNext();
      } else {
        return nullptr;
      }
    }
  }

  inline Record *GetNextBySize() {
    if (item_vec.size() == 0) {
      return nullptr;
    }

    auto old_it = item_it;
    if (item_it != item_vec.end()) {
      item_it += 1;
      return *old_it;
    } else {
      auto &last_record = item_vec.back();
      node = this->tree->TraverseToLeaf(nullptr,
                                        last_record->GetKey(),
                                        last_record->meta.GetKeyLength(),
                                        false);
      if (node) {
        item_vec.clear();
        const char *last_key = last_record->GetKey();
        uint32_t last_len = last_record->meta.GetKeyLength();
        node->RangeScanBySize(last_key, last_len, &scan_size, &item_vec, tree->pmwcas_pool);

        // Exclude the first which is the previous key
        item_it = item_vec.begin();
        Record *r = *item_it;
        if (KeyCompare(last_key, last_len, r->GetKey(), r->meta.GetKeyLength()) == 0) {
          ++item_it;
        }

        if (item_it == item_vec.end()) {
          return nullptr;
        }
        return GetNext();
      } else {
        return nullptr;
      }
    }
  }

  inline Record *GetNext() {
    return by_key ? GetNextByKey() : GetNextBySize();
  }

 private:
  BzTree *tree;
  const char *begin_key;
  uint16_t begin_size;
  const char *end_key;
  uint16_t end_size;
  LeafNode *node;
  uint32_t scan_size;
  bool by_key;
  std::vector<Record *> item_vec;
  std::vector<Record *>::iterator item_it;
};

}  // namespace bztree
