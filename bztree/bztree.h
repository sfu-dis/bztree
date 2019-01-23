// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#pragma once

#include <vector>
#include <memory>
#include <optional>

#include "include/pmwcas.h"
#include "mwcas/mwcas.h"

namespace bztree {

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

  static ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }
  static ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }
  static ReturnCode PMWCASFailure() { return ReturnCode(RetPMWCASFail); }
  static ReturnCode Ok() { return ReturnCode(RetOk); }
  static ReturnCode NotFound() { return ReturnCode(RetNotFound); }
  static ReturnCode NotEnoughSpace() { return ReturnCode(RetNotEnoughSpace); }
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

    static const uint64_t kControlMask = uint64_t{0x7} << 61; // Bits 64-62
    static const uint64_t kFrozenMask = uint64_t{0x1} << 60; // Bit 61
    static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
    static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;   // Bits 44-23
    static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;  // Bits 22-1

    inline void Freeze() { word |= kFrozenMask; }
    inline bool IsFrozen() { return (word & kFrozenMask) > 0; }
    inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }
    inline void SetRecordCount(uint16_t count) {
      word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
    }
    inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }
    inline void SetBlockSize(uint32_t size) {
      word = (word & (~kBlockSizeMask)) | (uint64_t{size} << 22);
    }
    inline uint32_t GetDeleteSize() { return (uint32_t) (word & kDeleteSizeMask); }
    inline void SetDeleteSize(uint32_t size) {
      word = (word & (~kDeleteSizeMask)) | uint64_t{size};
    }

    inline void PrepareForInsert(uint32_t size) {
      // Increment [record count] by one and [block size] by payload size
      word += ((uint64_t{1} << 44) + (uint64_t{size} << 22));
    }
  };

  uint32_t size;
  StatusWord status;
  uint32_t sorted_count;
  NodeHeader() : size(0), sorted_count(0) {}
  inline StatusWord GetStatus(pmwcas::EpochManager *epoch) {
    auto status_val = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        &this->status.word)->GetValue(epoch);
    return StatusWord{status_val};
  }
};

struct RecordMetadata {
  uint64_t meta;
  RecordMetadata() : meta(0) {}
  explicit RecordMetadata(uint64_t meta) : meta(meta) {}

  static const uint64_t kControlMask = uint64_t{0x7} << 61; // Bits 64-62
  static const uint64_t kVisibleMask = uint64_t{0x1} << 60; // Bit 61
  static const uint64_t kOffsetMask = uint64_t{0xFFFFFFF} << 32;     // Bits 60-33
  static const uint64_t kKeyLengthMask = uint64_t{0xFFFF} << 16;    // Bits 32-17
  static const uint64_t kTotalLengthMask = uint64_t{0xFFFF};  // Bits 16-1

  static const uint64_t kAllocationEpochMask = uint64_t{0x7FFFFFF} << 32;  // Bit 59-33

  inline bool IsVacant() { return meta == 0; }
  inline uint16_t GetKeyLength() const { return (uint16_t) ((meta & kKeyLengthMask) >> 16); }

//    Get the padded key length from accurate key length
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
  inline void PrepareForInsert(uint64_t epoch) {
    assert(IsVacant());
    // This only has to do with the offset field, which serves the dual
    // purpose of (1) storing a true record offset, and (2) storing the
    // allocation epoch used for recovery. The high-order bit of the offset
    // field indicates whether it is (1) or (2).
    //
    // Flip the high order bit of [offset] to indicate this field contains an
    // allocation epoch and fill in the rest offset bits with global epoch
    assert(epoch < (uint64_t{1} << 27));
    meta = (uint64_t{1} << 59) | (epoch << 32);
    assert(IsInserting(epoch));
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
  inline bool IsInserting(uint64_t epoch_index) {
    // record is not visible
    // and record allocation epoch equal to global index epoch
    return !IsVisible() && OffsetIsEpoch() &&
        ((meta & kAllocationEpochMask) >> 32 == epoch_index);
  }
};

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
    auto cmp = memcmp(key1, key2, std::min<uint32_t>(size1, size2));
    if (cmp == 0) {
      return size1 - size2;
    }
    return cmp;
  }
  // Set the frozen bit to prevent future modifications to the node
  bool Freeze(pmwcas::DescriptorPool *pmwcas_pool);
  inline RecordMetadata GetMetadata(uint32_t i, pmwcas::EpochManager *epoch) {
    // ensure the metadata is installed
    auto meta = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        record_metadata + i)->GetValue(epoch);
    return RecordMetadata{meta};
  }
  explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
    header.size = size;
  }
  inline bool IsLeaf() { return is_leaf; }
  inline NodeHeader *GetHeader() { return &header; }
//  Return a meta (not deleted) or nullptr (deleted or not exist)
//  It's user's responsibility to check IsInserting()
//  if check_concurrency is false, it will ignore all inserting record
  RecordMetadata *SearchRecordMeta(pmwcas::EpochManager *epoch,
                                   const char *key, uint32_t key_size,
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
                           pmwcas::EpochManager *epoch, bool safe_get = false) {
    if (!meta.IsVisible()) {
      return false;
    }
    assert(meta.GetTotalLength());
    char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
    if (data != nullptr) {
      *data = tmp_data;
    }
    auto padded_key_len = meta.GetPaddedKeyLength();
    if (key != nullptr) {
//    zero key length dummy record
      *key = padded_key_len == 0 ? nullptr : tmp_data;
    }

    if (payload != nullptr) {
      uint64_t tmp_payload;
      if (safe_get) {
        tmp_payload = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
            tmp_data + padded_key_len)->GetValue(epoch);
      } else {
        tmp_payload = *reinterpret_cast<uint64_t *> (tmp_data + padded_key_len);
      }
      *payload = tmp_payload;
    }
    return true;
  }
};

class Stack;

// Internal node: immutable once created, no free space, keys are always sorted
class InternalNode : public BaseNode {
 public:
  static InternalNode *New(InternalNode *src_node, const char *key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           pmwcas::EpochManager *epoch);
  static InternalNode *New(const char *key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           pmwcas::EpochManager *epoch);
  InternalNode *New(InternalNode *src_node, uint32_t begin_meta_idx, uint32_t nr_records,
                    const char *key, uint32_t key_size,
                    uint64_t left_child_addr, uint64_t right_child_addr,
                    pmwcas::EpochManager *epoch,
                    uint64_t left_most_child_addr = 0);

  InternalNode(uint32_t node_size, const char *key, uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr,
               pmwcas::EpochManager *epoch);
  InternalNode(uint32_t node_size, InternalNode *src_node,
               uint32_t begin_meta_idx, uint32_t nr_records,
               const char *key, uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr,
               pmwcas::EpochManager *epoch,
               uint64_t left_most_child_addr = 0);
  ~InternalNode() = default;

  InternalNode *PrepareForSplit(Stack &stack, uint32_t split_threshold,
                                const char *key, uint32_t key_size,
                                uint64_t left_child_addr, uint64_t right_child_addr,
                                pmwcas::DescriptorPool *pool);

  inline uint64_t *GetPayloadPtr(RecordMetadata meta) {
    char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
    return reinterpret_cast<uint64_t *>(ptr);
  }
  ReturnCode Update(RecordMetadata meta, InternalNode *old_child, InternalNode *new_child,
                    pmwcas::DescriptorPool *pmwcas_pool);
  uint32_t GetChildIndex(const char *key, uint16_t key_size,
                         pmwcas::DescriptorPool *pool, bool get_le = true);
  inline BaseNode *GetChildByMetaIndex(uint32_t index, pmwcas::EpochManager *epoch) {
    uint64_t child_addr;
    GetRawRecord(GetMetadata(index, epoch), nullptr, nullptr, &child_addr, epoch, true);
    return reinterpret_cast<BaseNode *> (child_addr);
  }
  void Dump(pmwcas::EpochManager *epoch, bool dump_children = false);
};

class LeafNode;
class BzTree;
struct Stack {
  struct Frame {
    Frame() : node(nullptr), meta() {}
    ~Frame() {}
    InternalNode *node;
    RecordMetadata meta;
  };
  static const uint32_t kMaxFrames = 32;
  Frame frames[kMaxFrames];
  uint32_t num_frames;
  BzTree *tree;

  Stack() : num_frames(0) {}
  ~Stack() { num_frames = 0; }
  inline void Push(InternalNode *node, RecordMetadata meta) {
    auto &frame = frames[num_frames++];
    frame.node = node;
    frame.meta = meta;
  }
  inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }
  inline void Clear() { num_frames = 0; }
  inline bool IsEmpty() { return num_frames == 0; }
  inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }
  inline InternalNode *GetRoot() { return num_frames > 0 ? frames[0].node : nullptr; }
};

struct Record;

class LeafNode : public BaseNode {
 public:
  static LeafNode *New(uint32_t node_size);

  static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
    return sizeof(LeafNode) + status.GetBlockSize() +
        status.GetRecordCount() * sizeof(RecordMetadata);
  }

  explicit LeafNode(uint32_t node_size = 4096) : BaseNode(true, node_size) {}
  ~LeafNode() = default;

  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload,
                    pmwcas::DescriptorPool *pmwcas_pool, uint32_t split_threshold);
  InternalNode *PrepareForSplit(Stack &stack, uint32_t split_threshold,
                                pmwcas::DescriptorPool *pmwcas_pool,
                                LeafNode **left, LeafNode **right);

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

  ReturnCode RangeScan(const char *key1,
                       uint32_t size1,
                       const char *key2,
                       uint32_t size2,
                       std::vector<std::unique_ptr<Record>> *result,
                       pmwcas::DescriptorPool *pmwcas_pool);

  // Consolidate all records in sorted order
  LeafNode *Consolidate(pmwcas::DescriptorPool *pmwcas_pool);

  inline char *GetKey(RecordMetadata meta) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
  }

  // Specialized GetRawRecord for leaf node only (key can't be nullptr)
  inline bool GetRawRecord(RecordMetadata meta, char **key,
                           uint64_t *payload, pmwcas::EpochManager *epoch) {
    char *unused = nullptr;
    return BaseNode::GetRawRecord(meta, &unused, key, payload, epoch);
  }

  inline uint32_t GetFreeSpace(pmwcas::EpochManager *epoch) {
    auto status = header.GetStatus(epoch);
    assert(header.size >= GetUsedSpace(status));
    return header.size - GetUsedSpace(status);
  }

  // Make sure this node is freezed before calling this function
  uint32_t SortMetadataByKey(std::vector<RecordMetadata> &vec,
                             bool visible_only,
                             pmwcas::EpochManager *epoch);
  void Dump(pmwcas::EpochManager *epoch);

 private:
  enum Uniqueness { IsUnique, Duplicate, ReCheck, NodeFrozen };
  Uniqueness CheckUnique(const char *key, uint32_t key_size, pmwcas::EpochManager *epoch);
  Uniqueness RecheckUnique(const char *key,
                           uint32_t key_size,
                           uint32_t end_pos,
                           pmwcas::EpochManager *epoch);
};

struct Record {
  RecordMetadata meta;
  char data[0];

  explicit Record(RecordMetadata meta) {
    this->meta = meta;
  }

  static inline std::unique_ptr<Record> New(RecordMetadata meta, BaseNode *node,
                                            pmwcas::EpochManager *epoch) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    auto item_ptr = reinterpret_cast<Record *> (malloc(meta.GetTotalLength() + sizeof(meta)));
    memset(item_ptr, 0, meta.GetTotalLength() + sizeof(Record));
    auto item = std::make_unique<Record>(meta);
    auto source_addr = (reinterpret_cast<char *>(node) + meta.GetOffset());

    // Key will never be changed and it will not be a pmwcas descriptor
    // but payload is fixed length 8-byte value, can be updated by pmwcas
    memcpy(item->data,
           reinterpret_cast<char *>(node) + meta.GetOffset(),
           meta.GetPaddedKeyLength());
    auto payload = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        source_addr + meta.GetPaddedKeyLength())->GetValue(epoch);
    memcpy(item->data + meta.GetPaddedKeyLength(), &payload, sizeof(payload));
    return item;
  }

  const uint64_t GetPayload() {
    return *reinterpret_cast<uint64_t *>(data + meta.GetPaddedKeyLength());
  }
  const char *GetKey() const {
    return data;
  }

  bool operator<(const Record &out) {
    auto out_key = out.GetKey();
    auto cmp = BaseNode::KeyCompare(this->GetKey(), this->meta.GetKeyLength(),
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

  BzTree(const ParameterSet &param, pmwcas::DescriptorPool *pool)
      : parameters(param), root(nullptr), pmwcas_pool(pool) {
    root = LeafNode::New(param.leaf_node_size);
  }
  void Dump();
  inline pmwcas::DescriptorPool *GetPool() const {
    return pmwcas_pool;
  }
  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload);
  ReturnCode Update(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Upsert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Delete(const char *key, uint16_t key_size);
  std::unique_ptr<Iterator> RangeScan(const char *key1, uint16_t size1,
                                      const char *key2, uint16_t size2);
  LeafNode *TraverseToLeaf(Stack *stack, const char *key,
                           uint16_t key_size,
                           bool le_child = true);

  // typically used when a parent is freezed and we want to re-find a new one.
  BaseNode *TraverseToNode(Stack *stack, const char *key,
                           uint16_t key_size, BaseNode *stop_at);

 private:
  bool ChangeRoot(uint64_t expected_root_addr, InternalNode *new_root);
  ParameterSet parameters;
  BaseNode *root;
  pmwcas::DescriptorPool *pmwcas_pool;
  BaseNode *GetRootNodeSafe() {
    auto root_node = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
        &root)->GetValue(pmwcas_pool->GetEpoch());
    return reinterpret_cast< BaseNode *>(root_node);
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
    node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
    node->RangeScan(begin_key, begin_size, end_key, end_size, &item_vec, tree->GetPool());
    item_it = item_vec.begin();
  }

  Record *GetNext() {
    auto old_it = item_it;
    if (item_it != item_vec.end()) {
      item_it += 1;
      return (*old_it).get();
    } else {
      auto &last_record = item_vec.back();
      node = this->tree->TraverseToLeaf(nullptr,
                                        last_record->GetKey(),
                                        last_record->meta.GetKeyLength(),
                                        false);
      if (node != nullptr) {
        item_vec.clear();
        node->RangeScan(begin_key, begin_size, end_key, end_size, &item_vec, tree->GetPool());
        item_it = item_vec.begin();
        return GetNext();
      } else {
        return nullptr;
      }
    }
  }

 private:
  BzTree *tree;
  const char *begin_key;
  uint16_t begin_size;
  const char *end_key;
  uint16_t end_size;
  LeafNode *node;
  std::vector<std::unique_ptr<Record>> item_vec;
  std::vector<std::unique_ptr<Record>>::iterator item_it;
};

}  // namespace bztree
