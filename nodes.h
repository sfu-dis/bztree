#pragma once

#include "basenode.h"

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

class LeafNode : public BaseNode {
 public:
  static void New(LeafNode **mem, uint32_t node_size);

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


