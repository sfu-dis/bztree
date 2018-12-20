// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#pragma once

#include <vector>

#include "include/pmwcas.h"
#include "mwcas/mwcas.h"

namespace bztree {

struct ReturnCode {
  enum RC { RetInvalid, RetOk, RetKeyExists, RetNotFound, RetNodeFrozen, RetPMWCASFail };
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

  static ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }
  static ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }
  static ReturnCode PMWCASFailure() { return ReturnCode(RetPMWCASFail); }
  static ReturnCode Ok() { return ReturnCode(RetOk); }
  static ReturnCode NotFound() { return ReturnCode(RetNotFound); }
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

    static const uint64_t kControlMask = 0x7;                          // Bits 1-3
    static const uint64_t kFrozenMask = 0x8;                           // Bit 4
    static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 4;    // Bits 5-20
    static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 20;   // Bits 21-42
    static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 42;  // Bits 43-64

    static const uint64_t kFrozenFlag = 0x8;

    inline void Freeze() { word |= kFrozenFlag; }
    inline bool IsFrozen() { return word & kFrozenMask; }
    inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 4); }
    inline void SetRecordCount(uint16_t count) {
      word = (word & (~kRecordCountMask)) | (uint64_t{count} << 4);
    }
    inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 20); }
    inline void SetBlockSize(uint32_t size) {
      word = (word & (~kBlockSizeMask)) | (uint64_t{size} << 20);
    }
    inline uint32_t GetDeleteSize() { return (uint32_t) ((word & kDeleteSizeMask) >> 42); }
    inline void SetDeleteSize(uint32_t size) {
      word = (word & (~kDeleteSizeMask)) | (uint64_t{size} << 42);
    }

    inline void PrepareForInsert(uint32_t size) {
      // Increment [record count] by one and [block size] by payload size
      word += ((uint64_t{1} << 4) + (uint64_t{size} << 20));
    }
  };

  static const uint32_t size = 4096;
  StatusWord status;
  uint32_t sorted_count;
  NodeHeader() : sorted_count(0) {}
};
struct RecordMetadata {
  uint64_t meta;
  RecordMetadata() : meta(0) {}

  static const uint64_t kControlMask = 0x7;                         // Bits 1-3
  static const uint64_t kVisibleMask = 0x8;                         // Bit 4
  static const uint64_t kOffsetMask = uint64_t{0xFFFFFFF} << 4;     // Bits 5-32
  static const uint64_t kKeyLengthMask = uint64_t{0xFFFF} << 32;    // Bits 33-48
  static const uint64_t kTotalLengthMask = uint64_t{0xFFFF} << 48;  // Bits 49-64

  static const uint64_t kVisibleFlag = 0x8;

  inline bool IsVacant() { return meta == 0; }
  inline uint16_t GetKeyLength() { return (uint16_t) ((meta & kKeyLengthMask) >> 32); }

//    Get the padded key length from accurate key length
  inline uint16_t GetPaddedKeyLength() {
    auto key_length = GetKeyLength();
    return PadKeyLength(key_length);
  }

  static inline constexpr uint16_t PadKeyLength(uint16_t key_length) {
    return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
  }
  inline uint16_t GetTotalLength() { return (uint16_t) ((meta & kTotalLengthMask) >> 48); }
  inline uint32_t GetOffset() { return (uint32_t) ((meta & kOffsetMask) >> 4); }
  inline bool OffsetIsEpoch() {
    return (GetOffset() >> 27) == 1;
  }
  inline void SetOffset(uint32_t offset) {
    meta = (meta & (~kOffsetMask)) | (offset << 4);
  }
  inline bool IsVisible() { return meta & kVisibleMask; }
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
    meta = (((uint64_t{1} << 27) | epoch) << 31);
  }
  inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint64_t total_len) {
    // Set the actual offset, the visible bit, key/total length
    meta = (offset << 4) | kVisibleFlag | (key_len << 32) | (total_len << 48);
    assert(GetKeyLength() == key_len);
  }
  inline bool IsInserting() {
    // record is not visible
    // and record allocation epoch equal to global index epoch
    // FIXME(hao): Check the Global index epoch
    return !IsVisible() && OffsetIsEpoch();
  }
};

class BaseNode {
 public:
  static const uint32_t kNodeSize = 4096;

 protected:
  bool is_leaf;
  NodeHeader header;
  RecordMetadata record_metadata[0];

 protected:
  // Set the frozen bit to prevent future modifications to the node
  bool Freeze(pmwcas::DescriptorPool *pmwcas_pool);
  inline RecordMetadata GetMetadata(uint32_t i) { return record_metadata[i]; }
  void Dump();

 public:
  explicit BaseNode(bool leaf) : is_leaf(leaf) {}
  inline bool IsLeaf() { return is_leaf; }
  inline NodeHeader *GetHeader() { return &header; }
//  Return a meta (not deleted) or nullptr (deleted or not exist)
//  It's user's responsibility to check IsInserting()
//  if check_concurrency is false, it will ignore all inserting record
  RecordMetadata *SearchRecordMeta(const char *key,
                                   uint32_t key_size,
                                   uint32_t start_pos = 0,
                                   uint32_t end_pos = (uint32_t) -1,
                                   bool check_concurrency = true);
  // Get the key and payload (8-byte)
  // Return status
  inline bool GetRecord(RecordMetadata meta, char **key, uint64_t *payload) {
    if (!meta.IsVisible()) {
      return false;
    }
    uint64_t offset = meta.GetOffset();
//    zero key length dummy record
    *key = meta.GetPaddedKeyLength() == 0 ?
           nullptr : reinterpret_cast<char *>(this) + meta.GetOffset();
    *payload = *(reinterpret_cast<uint64_t *> (reinterpret_cast<char *>(this) +
        meta.GetOffset() + meta.GetPaddedKeyLength()));
    return true;
  }

  inline uint32_t GetFreeSpace() {
    return BaseNode::kNodeSize - sizeof(BaseNode) - header.status.GetBlockSize()
        - header.status.GetRecordCount() * sizeof(RecordMetadata);
  }
};

// Internal node: immutable once created, no free space, keys are always sorted
class InternalNode : public BaseNode {
 public:
  static InternalNode *New(InternalNode *src_node, char *key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr);
  static InternalNode *New(char *key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr);

  InternalNode(const char *key, const uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr);
  InternalNode(InternalNode *src_node, const char *key, const uint16_t key_size,
               uint64_t left_child_addr, uint64_t right_child_addr);
  ~InternalNode() = default;

  inline uint64_t *GetPayloadPtr(RecordMetadata meta) {
    char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
    return reinterpret_cast<uint64_t *>(ptr);
  }
  ReturnCode Update(RecordMetadata meta, InternalNode *old_child, InternalNode *new_child,
                    pmwcas::DescriptorPool *pmwcas_pool);
  BaseNode *GetChild(const char *key, uint16_t key_size, RecordMetadata *out_meta = nullptr);
  void Dump(bool dump_children = false);
};

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

  Stack() : num_frames(0) {}
  ~Stack() { num_frames = 0; }
  inline void Push(InternalNode *node, RecordMetadata meta) {
    auto &frame = frames[num_frames++];
    frame.node = node;
    frame.meta = meta;
  }
  inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }
  inline void Clear() { num_frames = 0; }
  inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }
};

class LeafNode : public BaseNode {
 public:
  static LeafNode *New();

  LeafNode() : BaseNode(true) {
  }
  ~LeafNode() = default;

  ReturnCode Insert(uint32_t epoch, const char *key, uint16_t key_size, uint64_t payload,
                    pmwcas::DescriptorPool *pmwcas_pool);
  ReturnCode PrepareForSplit(uint32_t epoch, Stack &stack,
                             InternalNode **parent, LeafNode **left, LeafNode **right,
                             pmwcas::DescriptorPool *pmwcas_pool);

  // Initialize new, empty node with a list of records; no concurrency control;
  // only useful before any inserts to the node. For now the only users are split
  // (when preparing a new node) and consolidation.
  //
  // The list of records to be inserted is specified through iterators of a
  // record metadata vector. Recods covered by [begin_it, end_it) will be
  // inserted to the node. Note end_it is non-inclusive.
  void CopyFrom(LeafNode *node,
                std::vector<RecordMetadata>::iterator begin_it,
                std::vector<RecordMetadata>::iterator end_it);

  ReturnCode Update(uint32_t epoch, const char *key, uint16_t key_size, uint64_t payload,
                    pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode Upsert(uint32_t epoch, const char *key, uint16_t key_size, uint64_t payload,
                    pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode Delete(const char *key, uint16_t key_size, pmwcas::DescriptorPool *pmwcas_pool);

  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload);
  // Consolidate all records in sorted order
  LeafNode *Consolidate(pmwcas::DescriptorPool *pmwcas_pool);

  inline char *GetKey(RecordMetadata meta) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
  }

  uint32_t SortMetadataByKey(std::vector<RecordMetadata> &vec, bool visible_only);
  void Dump();

 private:
  enum Uniqueness { IsUnique, Duplicate, ReCheck };
  Uniqueness CheckUnique(const char *key, uint32_t key_size);
  Uniqueness RecheckUnique(const char *key, uint32_t key_size, uint32_t end_pos);
};

class BzTree {
 public:
  struct ParameterSet {
    uint32_t split_threshold;
    uint32_t merge_threshold;
    ParameterSet() : split_threshold(3072), merge_threshold(1024) {}
    ParameterSet(uint32_t split_threshold, uint32_t merge_threshold)
        : split_threshold(split_threshold), merge_threshold(merge_threshold) {}
    ~ParameterSet() {}
  };

  BzTree(ParameterSet param, pmwcas::DescriptorPool *pool)
      : parameters(param), epoch(0), root(nullptr), pmwcas_pool(pool) {
    root = LeafNode::New();
  }
  void Dump();
  ReturnCode Insert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Read(const char *key, uint16_t key_size, uint64_t *payload);
  ReturnCode Update(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Upsert(const char *key, uint16_t key_size, uint64_t payload);
  ReturnCode Delete(const char *key, uint16_t key_size);

 private:
  LeafNode *TraverseToLeaf(Stack &stack, const char *key, uint64_t key_size);

 private:
  ParameterSet parameters;
  uint32_t epoch;
  BaseNode *root;
  pmwcas::DescriptorPool *pmwcas_pool;
};

}  // namespace bztree
