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

#ifndef ALWAYS_ASSERT
#define ALWAYS_ASSERT(expr) (expr) ? (void)0 : abort()
#endif

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

extern uint64_t global_epoch;

struct ReturnCode {
  enum RC {
    RetInvalid,
    RetOk,
    RetKeyExists,
    RetNotFound,
    RetNodeFrozen,
    RetPMWCASFail,
    RetNotEnoughSpace
  };

  uint8_t rc;

  constexpr explicit ReturnCode(uint8_t r) : rc(r) {}
  constexpr ReturnCode() : rc(RetInvalid) {}
  ~ReturnCode() = default;

  constexpr bool inline IsInvalid() const { return rc == RetInvalid; }
  constexpr bool inline IsOk() const { return rc == RetOk; }
  constexpr bool inline IsKeyExists() const { return rc == RetKeyExists; }
  constexpr bool inline IsNotFound() const { return rc == RetNotFound; }
  constexpr bool inline IsNodeFrozen() const { return rc == RetNodeFrozen; }
  constexpr bool inline IsPMWCASFailure() const { return rc == RetPMWCASFail; }
  constexpr bool inline IsNotEnoughSpace() const { return rc == RetNotEnoughSpace; }

  static inline ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }
  static inline ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }
  static inline ReturnCode PMWCASFailure() { return ReturnCode(RetPMWCASFail); }
  static inline ReturnCode Ok() { return ReturnCode(RetOk); }
  static inline ReturnCode NotFound() { return ReturnCode(RetNotFound); }
  static inline ReturnCode NotEnoughSpace() { return ReturnCode(RetNotEnoughSpace); }
};

struct NodeHeader {
  // Header:
  // |-------64 bits-------|---32 bits---|---32 bits---|
  // |     status word     |     size    | sorted count|
  //
  // Sorted count is actually the index into the first metadata entry for
  // unsorted records. Following the header is a growing array of record metadata
  // entries.

  // 64-bit status word subdivided into five fields. Internal nodes only use the
  // first two (control and frozen) while leaf nodes use all the five.
  struct StatusWord {
    uint64_t word;
    StatusWord() : word(0) {}
    explicit StatusWord(uint64_t word) : word(word) {}

    static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
    static const uint64_t kFrozenMask = uint64_t{0x1} << 60;            // Bit 61
    static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
    static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;    // Bits 44-23
    static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;    // Bits 22-1

    inline StatusWord Freeze() {
      return StatusWord{word | kFrozenMask};
    }
    inline bool IsFrozen() { return (word & kFrozenMask) > 0; }
    inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }
    inline void SetRecordCount(uint16_t count) {
      word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
    }
    inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }
    inline void SetBlockSize(uint32_t size) {
      word = (word & (~kBlockSizeMask)) | (uint64_t{size} << 22);
    }
    inline uint32_t GetDeletedSize() { return (uint32_t) (word & kDeleteSizeMask); }
    inline void SetDeleteSize(uint32_t size) {
      word = (word & (~kDeleteSizeMask)) | uint64_t{size};
    }

    inline void PrepareForInsert(uint32_t size) {
      ALWAYS_ASSERT(size > 0);
      // Increment [record count] by one and [block size] by payload size
      word += ((uint64_t{1} << 44) + (uint64_t{size} << 22));
    }
  };

  uint32_t size;
  StatusWord status;
  uint32_t sorted_count;
  NodeHeader() : size(0), sorted_count(0) {}
  inline StatusWord GetStatus() {
    auto status_val = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        &this->status.word)->GetValueProtected();
    return StatusWord{status_val};
  }
};

struct RecordMetadata {
  uint64_t meta;
  RecordMetadata() : meta(0) {}
  explicit RecordMetadata(uint64_t meta) : meta(meta) {}

  static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
  static const uint64_t kVisibleMask = uint64_t{0x1} << 60;           // Bit 61
  static const uint64_t kOffsetMask = uint64_t{0xFFFFFFF} << 32;      // Bits 60-33
  static const uint64_t kKeyLengthMask = uint64_t{0xFFFF} << 16;      // Bits 32-17
  static const uint64_t kTotalLengthMask = uint64_t{0xFFFF};          // Bits 16-1

  static const uint64_t kAllocationEpochMask = uint64_t{0x7FFFFFF} << 32;  // Bit 59-33

  inline bool IsVacant() { return meta == 0; }
  inline uint16_t GetKeyLength() const { return (uint16_t) ((meta & kKeyLengthMask) >> 16); }

  // Get the padded key length from accurate key length
  inline uint16_t GetPaddedKeyLength() {
    auto key_length = GetKeyLength();
    return PadKeyLength(key_length);
  }

  static inline constexpr uint16_t PadKeyLength(uint16_t key_length) {
    return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
  }
  inline uint16_t GetTotalLength() { return (uint16_t) (meta & kTotalLengthMask); }
  inline uint32_t GetOffset() { return (uint32_t) ((meta & kOffsetMask) >> 32); }
  inline bool OffsetIsEpoch() {
    return (GetOffset() >> 27) == 1;
  }
  inline void SetOffset(uint32_t offset) {
    meta = (meta & (~kOffsetMask)) | (uint64_t{offset} << 32);
  }
  inline bool IsVisible() { return (meta & kVisibleMask) > 0; }
  inline void SetVisible(bool visible) {
    if (visible) {
      meta = meta | kVisibleMask;
    } else {
      meta = meta & (~kVisibleMask);
    }
  }
  inline void PrepareForInsert() {
    assert(IsVacant());
    // This only has to do with the offset field, which serves the dual
    // purpose of (1) storing a true record offset, and (2) storing the
    // allocation epoch used for recovery. The high-order bit of the offset
    // field indicates whether it is (1) or (2).
    //
    // Flip the high order bit of [offset] to indicate this field contains an
    // allocation epoch and fill in the rest offset bits with global epoch
    assert(global_epoch < (uint64_t{1} << 27));
    meta = (uint64_t{1} << 59) | (global_epoch << 32);
    assert(IsInserting());
  }
  inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint64_t total_len) {
    // Set the actual offset, the visible bit, key/total length
    if (offset == 0) {
      // this record is duplicate inserted
      // make it invisible
      meta = (offset << 32) | (uint64_t{0} << 60) | (key_len << 16) | total_len;
    } else {
      meta = (offset << 32) | kVisibleMask | (key_len << 16) | total_len;
    }
    assert(GetKeyLength() == key_len);
  }
  inline bool IsInserting() {
    // record is not visible
    // and record allocation epoch equal to global index epoch
    return !IsVisible() && OffsetIsEpoch() &&
        (((meta & kAllocationEpochMask) >> 32) == global_epoch);
  }
};

static const inline int my_memcmp(const char *key1, const char *key2, uint32_t size) {
  for (uint32_t i = 0; i < size; i++) {
    if (key1[i] != key2[i]) {
      return key1[i] - key2[i];
    }
  }
  return 0;
}

class Stack;
class BaseNode {
 protected:
  bool is_leaf;
  NodeHeader header;
  RecordMetadata record_metadata[0];
  void Dump(pmwcas::EpochManager *epoch);

  // Check if the key in a range, inclusive
  // -1 if smaller than left key
  // 1 if larger than right key
  // 0 if in range
  static const inline int KeyInRange(const char *key, uint32_t size,
                                     const char *key_left, uint32_t size_left,
                                     const char *key_right, uint32_t size_right) {
    auto cmp = KeyCompare(key_left, size_left, key, size);
    if (cmp > 0) {
      return -1;
    }
    cmp = KeyCompare(key, size, key_right, size_right);
    if (cmp <= 0) {
      return 0;
    } else {
      return 1;
    }
  }

 public:
  static const inline int KeyCompare(const char *key1, uint32_t size1,
                                     const char *key2, uint32_t size2) {
    if (!key1) {
      return -1;
    } else if (!key2) {
      return 1;
    }
    int cmp;
    if (std::min(size1, size2) < 16) {
      cmp = my_memcmp(key1, key2, std::min<uint32_t>(size1, size2));
    } else {
      cmp = memcmp(key1, key2, std::min<uint32_t>(size1, size2));
    }
    if (cmp == 0) {
      return size1 - size2;
    }
    return cmp;
  }
  // Set the frozen bit to prevent future modifications to the node
  bool Freeze(pmwcas::DescriptorPool *pmwcas_pool);
  inline RecordMetadata GetMetadata(uint32_t i) {
    // ensure the metadata is installed
    auto meta = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        record_metadata + i)->GetValueProtected();
    return RecordMetadata{meta};
  }
  explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
    header.size = size;
  }
  inline bool IsLeaf() { return is_leaf; }
  inline NodeHeader *GetHeader() { return &header; }

  // Return a meta (not deleted) or nullptr (deleted or not exist)
  // It's user's responsibility to check IsInserting()
  // if check_concurrency is false, it will ignore all inserting record
  RecordMetadata SearchRecordMeta(pmwcas::EpochManager *epoch,
                                  const char *key, uint32_t key_size,
                                  RecordMetadata **out_metadata,
                                  uint32_t start_pos = 0,
                                  uint32_t end_pos = (uint32_t) -1,
                                  bool check_concurrency = true);

  // Get the key and payload (8-byte), not thread-safe
  // Outputs:
  // 1. [*data] - pointer to the char string that stores key followed by payload.
  //    If the record has a null key, then this will point directly to the
  //    payload
  // 2. [*key] - pointer to the key (could be nullptr)
  // 3. [payload] - 8-byte payload
  inline bool GetRawRecord(RecordMetadata meta, char **data, char **key, uint64_t *payload,
                           pmwcas::EpochManager *epoch = nullptr) {
    assert(meta.GetTotalLength());
    char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
    if (data != nullptr) {
      *data = tmp_data;
    }
    auto padded_key_len = meta.GetPaddedKeyLength();
    if (key != nullptr) {
      // zero key length dummy record
      *key = padded_key_len == 0 ? nullptr : tmp_data;
    }

    if (payload != nullptr) {
      uint64_t tmp_payload;
      if (epoch != nullptr) {
        tmp_payload = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
            tmp_data + padded_key_len)->GetValueProtected();
      } else {
        tmp_payload = *reinterpret_cast<uint64_t *> (tmp_data + padded_key_len);
      }
      *payload = tmp_payload;
    }
    return true;
  }

  inline char *GetKey(RecordMetadata meta) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
  }

  inline bool IsFrozen() {
    return GetHeader()->GetStatus().IsFrozen();
  }

  ReturnCode CheckMerge(Stack *stack, const char *key, uint32_t key_size, bool backoff);
};

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
                       pmwcas::DescriptorPool *pool, bool backoff);

  inline uint64_t *GetPayloadPtr(RecordMetadata meta) {
    char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
    return reinterpret_cast<uint64_t *>(ptr);
  }
  ReturnCode Update(RecordMetadata meta, InternalNode *old_child, InternalNode *new_child,
                    pmwcas::Descriptor *pd, pmwcas::DescriptorPool *pmwcas_pool);
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
                    pmwcas::DescriptorPool *pmwcas_pool, uint32_t split_threshold);
  bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                       pmwcas::Descriptor *pd,
                       pmwcas::DescriptorPool *pmwcas_pool,
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
                    pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode Delete(const char *key, uint16_t key_size, pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload,
                  pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode RangeScanByKey(const char *key1,
                            uint32_t size1,
                            const char *key2,
                            uint32_t size2,
                            std::vector<Record *> *result,
                            pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode RangeScanBySize(const char *key1,
                             uint32_t size1,
                             uint32_t to_scan,
                             std::list<std::unique_ptr<Record>> *result,
                             pmwcas::DescriptorPool *pmwcas_pool);

  // Consolidate all records in sorted order
  LeafNode *Consolidate(pmwcas::DescriptorPool *pmwcas_pool);

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
  BzTree(const ParameterSet &param, pmwcas::DescriptorPool *pool, uint64_t pmdk_addr = 0)
      : parameters(param), root(nullptr), pmdk_addr(pmdk_addr), index_epoch(0) {
    global_epoch = index_epoch;
    SetPMWCASPool(pool);
    pmwcas::EpochGuard guard(GetPMWCASPool()->GetEpoch());
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
    pmwcas::DescriptorPool *pool = GetPMWCASPool();
    pool->Recovery(false);

    pmwcas::NVRAM::Flush(sizeof(bztree::BzTree), this);
  }
#endif

  void Dump();

  inline static BzTree *New(const ParameterSet &param, pmwcas::DescriptorPool *pool) {
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

  void SetPMWCASPool(pmwcas::DescriptorPool *pool) {
#ifdef PMDK
    this->pmwcas_pool = Allocator::Get()->GetOffset(pool);
#else
    this->pmwcas_pool = pool;
#endif
  }

  inline pmwcas::DescriptorPool *GetPMWCASPool() {
#ifdef PMDK
    return Allocator::Get()->GetDirect(pmwcas_pool);
#else
    return pmwcas_pool;
#endif
  }

  inline uint64_t GetPMDKAddr() {
    return pmdk_addr;
  }

  inline uint64_t GetEpoch() {
    return index_epoch;
  }

  ParameterSet parameters;
  bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr, pmwcas::Descriptor *pd);

 private:
  BaseNode *root;
  pmwcas::DescriptorPool *pmwcas_pool;
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
  explicit Iterator(BzTree *tree, const char *begin_key, uint16_t begin_size, uint32_t scan_size) :
      key(begin_key), size(begin_size), tree(tree), remaining_size(scan_size) {
    node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
    node->RangeScanBySize(begin_key, begin_size, scan_size, &item_vec, tree->GetPMWCASPool());
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
    node->RangeScanBySize(last_key, last_len, remaining_size, &item_vec, tree->GetPMWCASPool());

    // FIXME(hao): this a temp workaround
    // should fix traverse to leaf instead
    // check if we hit the same record
    if (!item_vec.empty()) {
      auto new_front = item_vec.front().get();
      if (BaseNode::KeyCompare(new_front->GetKey(), new_front->meta.GetKeyLength(),
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
