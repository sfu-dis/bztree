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

#ifndef ALWAYS_ASSERT
#define ALWAYS_ASSERT(expr) (expr) ? (void)0 : abort()
#endif

template<typename T>
using nv_ptr= pmwcas::nv_ptr<T>;

namespace bztree {

#ifdef PMDK
struct Allocator {
  static pmwcas::PMDKAllocator *allocator_;
  static void Init(pmwcas::PMDKAllocator *allocator) {
    allocator_ = allocator;
  }
  inline static pmwcas::PMDKAllocator *Get() {
    return allocator_;
  }
};
#endif

// Internal node: immutable once created, no free space, keys are always sorted
// operations that might mutate the InternalNode:
//    a. create a new node, this will set the freeze bit in status
//    b. update a pointer, this will check the status field and swap in a new pointer
// in both cases, the record metadata should not be touched,
// thus we can safely dereference them without a wrapper.
class InternalNode : public BaseNode {
 public:
  static void New(InternalNode *src_node, const char *key, uint32_t key_size,
                  uint64_t left_child_addr, uint64_t right_child_addr,
                  InternalNode **mem);
  static void New(const char *key, uint32_t key_size,
                  uint64_t left_child_addr, uint64_t right_child_addr,
                  InternalNode **mem);
  static void New(InternalNode *src_node, uint32_t begin_meta_idx, uint32_t nr_records,
                  const char *key, uint32_t key_size,
                  uint64_t left_child_addr, uint64_t right_child_addr,
                  InternalNode **mem,
                  uint64_t left_most_child_addr);
  static void New(InternalNode **mem, uint32_t node_size);

  InternalNode(uint32_t node_size, const char *key, uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr);
  InternalNode(uint32_t node_size, InternalNode *src_node,
               uint32_t begin_meta_idx, uint32_t nr_records,
               const char *key, uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr,
               uint64_t left_most_child_addr = 0);
  ~InternalNode() = default;

  bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                       const char *key, uint32_t key_size,
                       uint64_t left_child_addr, uint64_t right_child_addr,
                       InternalNode **new_node, pmwcas::Descriptor *pd,
                       nv_ptr<pmwcas::DescriptorPool> pool, bool backoff);

  inline uint64_t *GetPayloadPtr(RecordMetadata meta) {
    char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
    return reinterpret_cast<uint64_t *>(ptr);
  }
  ReturnCode Update(RecordMetadata meta, InternalNode *old_child, InternalNode *new_child,
                    pmwcas::Descriptor *pd);
  uint32_t GetChildIndex(const char *key, uint16_t key_size, bool get_le = true);

  // epoch here is required: record ptr might be a desc due to UPDATE operation
  // but record_metadata don't need a epoch
  inline BaseNode *GetChildByMetaIndex(uint32_t index, pmwcas::EpochManager *epoch) {
    uint64_t child_addr;
    GetRawRecord(record_metadata[index], nullptr, nullptr, &child_addr, epoch);

#ifdef PMDK
    return Allocator::Get()->GetDirect<BaseNode>(reinterpret_cast<BaseNode *> (child_addr));
#else
    return reinterpret_cast<BaseNode *> (child_addr);
#endif
  }
  void Dump(pmwcas::EpochManager *epoch, bool dump_children = false);

  // delete a child from internal node
  // | key0, val0 | key1, val1 | key2, val2 | key3, val3 |
  // ==>
  // | key0, val0 | key1, val1' | key3, val3 |
  void DeleteRecord(uint32_t meta_to_update,
                    uint64_t new_child_ptr,
                    InternalNode **new_node);

  static bool MergeNodes(InternalNode *left_node, InternalNode *right_node,
                         const char *key, uint32_t key_size, InternalNode **new_node);
};

class LeafNode;
class BzTree;
struct Stack {
  struct Frame {
    Frame() : node(nullptr), meta_index() {}
    ~Frame() {}
    InternalNode *node;
    uint32_t meta_index;
  };
  static const uint32_t kMaxFrames = 32;
  Frame frames[kMaxFrames];
  uint32_t num_frames;
  BzTree *tree;
  BaseNode *root;

  Stack() : num_frames(0) {}
  ~Stack() { num_frames = 0; }

  inline void Push(InternalNode *node, uint32_t meta_index) {
    ALWAYS_ASSERT(num_frames < kMaxFrames);
    auto &frame = frames[num_frames++];
    frame.node = node;
    frame.meta_index = meta_index;
  }
  inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }
  inline void Clear() {
    root = nullptr;
    num_frames = 0;
  }
  inline bool IsEmpty() { return num_frames == 0; }
  inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }
  inline BaseNode *GetRoot() { return root; }
  inline void SetRoot(BaseNode *node) { root = node; }
};

struct Record;

class LeafNode : public BaseNode {
 public:
  static void New(LeafNode **mem, uint32_t node_size);

  static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
    return sizeof(LeafNode) + status.GetBlockSize() +
        status.GetRecordCount() * sizeof(RecordMetadata);
  }

  explicit LeafNode(uint32_t node_size = 4096) : BaseNode(true, node_size) {}
  ~LeafNode() = default;

  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload,
                    nv_ptr<pmwcas::DescriptorPool> pmwcas_pool, uint32_t split_threshold);
  bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                       pmwcas::Descriptor *pd,
                       nv_ptr<pmwcas::DescriptorPool> pmwcas_pool,
                       LeafNode **left, LeafNode **right,
                       InternalNode **new_parent, bool backoff);

  // merge two nodes into a new one
  // copy the meta/data to the new node
  static bool MergeNodes(LeafNode *left_node, LeafNode *right_node, LeafNode **new_node);

  // Initialize new, empty node with a list of records; no concurrency control;
  // only useful before any inserts to the node. For now the only users are split
  // (when preparing a new node) and consolidation.
  //
  // The list of records to be inserted is specified through iterators of a
  // record metadata vector. Recods covered by [begin_it, end_it) will be
  // inserted to the node. Note end_it is non-inclusive.
  void CopyFrom(LeafNode *node,
                std::vector<RecordMetadata>::iterator begin_it,
                std::vector<RecordMetadata>::iterator end_it,
                pmwcas::EpochManager *epoch);

  ReturnCode Update(const char *key, uint16_t key_size, uint64_t payload,
                    nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  ReturnCode Delete(const char *key, uint16_t key_size, nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload,
                  nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  ReturnCode RangeScanByKey(const char *key1,
                            uint32_t size1,
                            const char *key2,
                            uint32_t size2,
                            std::vector<Record *> *result,
                            nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  ReturnCode RangeScanBySize(const char *key1,
                             uint32_t size1,
                             uint32_t *to_scan,
                             std::vector<Record *> *result,
                             nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  // Consolidate all records in sorted order
  LeafNode *Consolidate(nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

  // Specialized GetRawRecord for leaf node only (key can't be nullptr)
  inline bool GetRawRecord(RecordMetadata meta, char **key,
                           uint64_t *payload, pmwcas::EpochManager *epoch = nullptr) {
    char *unused = nullptr;
    return BaseNode::GetRawRecord(meta, &unused, key, payload, epoch);
  }

  inline uint32_t GetFreeSpace() {
    auto status = header.GetStatus();
    assert(header.size >= GetUsedSpace(status));
    return header.size - GetUsedSpace(status);
  }

  // Make sure this node is frozen before calling this function
  uint32_t SortMetadataByKey(std::vector<RecordMetadata> &vec,
                             bool visible_only,
                             pmwcas::EpochManager *epoch);
  void Dump(pmwcas::EpochManager *epoch);

 private:
  enum Uniqueness { IsUnique, Duplicate, ReCheck, NodeFrozen };
  Uniqueness CheckUnique(const char *key, uint32_t key_size, pmwcas::EpochManager *epoch);
  Uniqueness RecheckUnique(const char *key,
                           uint32_t key_size,
                           uint32_t end_pos);
};

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
    int cmp = BaseNode::KeyCompare(this->GetKey(), this->meta.GetKeyLength(),
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
  BaseNode *TraverseToNode(bztree::Stack *stack,
                           const char *key, uint16_t key_size,
                           bztree::BaseNode *stop_at = nullptr,
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
        if (BaseNode::KeyCompare(last_key, last_len, r->GetKey(), r->meta.GetKeyLength()) == 0) {
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
