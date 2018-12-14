#pragma once
#include "include/pmwcas.h"
#include "mwcas/mwcas.h"

namespace bztree {

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

    static const uint64_t kControlMask = 0x7;                         // Bits 1-3
    static const uint64_t kFrozenMask = 0x8;                          // Bit 4
    static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 4;   // Bits 5-20
    static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 20;  // Bits 21-42
    static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 42; // Bits 43-64

    static const uint64_t kFrozenFlag = 0x8;

    inline void Freeze() { word |= kFrozenFlag; }
    inline bool IsFrozen() { return word & kFrozenMask; }
    inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 4); }
    inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 20); }
    inline uint32_t GetDeleteSize() { return (uint32_t) ((word & kDeleteSizeMask) >> 42); }
    inline void SetDeleteSize(uint32_t size) {
      word = (word & (~kDeleteSizeMask)) | (uint64_t{size} << 42);
    }
    inline void PrepareForInsert(uint32_t size) {
      // Increment [record count] by one and [block size] by payload size
      word += ((uint64_t{1} << 4) + (uint64_t{size} << 20));
    }
  };

  uint32_t size;
  StatusWord status;
  uint32_t sorted_count;
  NodeHeader() : size(0), sorted_count(0) {}
};

class BaseNode {
 public:
  struct RecordMetadata {
    uint64_t meta;
    RecordMetadata() : meta(0) {}

    static const uint64_t kControlMask = 0x7;                        // Bits 1-3
    static const uint64_t kVisibleMask = 0x8;                        // Bit 4
    static const uint64_t kOffsetMask = uint64_t{0xFFFFFFF} << 4;    // Bits 5-32
    static const uint64_t kKeyLengthMask = uint64_t{0xFFFF} << 32;   // Bits 33-48
    static const uint64_t kTotalLengthMask = uint64_t{0xFFFF} << 48; // Bits 49-64

    static const uint64_t kVisibleFlag = 0x8;

    inline bool IsVacant() { return meta == 0; }
    inline uint16_t GetKeyLength() { return (uint16_t) ((meta & kKeyLengthMask) >> 32); }
    inline uint16_t GetTotalLength() { return (uint16_t) ((meta & kTotalLengthMask) >> 48); }
    inline uint32_t GetOffset() { return (uint32_t) ((meta & kOffsetMask) >> 4); }
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
//    record is not visible
//    and record allocation epoch equal to global index epoch
//    FIXME(hao): Global index epoch may not be zero
      auto offset = GetOffset();
      return IsVisible() == 0 &&
          ((offset & uint64_t{0xFFFFFFF}) == 0);
    }
  };

 protected:
  bool is_leaf;
  NodeHeader header;
  RecordMetadata record_metadata[0];

 protected:
  // Set the frozen bit to prevent future modifications to the node
  bool Freeze(pmwcas::DescriptorPool *pmwcas_pool);

 public:
  BaseNode(bool leaf) : is_leaf(leaf) {}
  inline bool IsLeaf() { return is_leaf; }
};

// Internal node: immutable once created, no free space, keys are always sorted
class InternalNode : public BaseNode {
 public:
  static InternalNode *New(uint32_t data_size, uint32_t sorted_count);

  InternalNode(uint64_t data_size, uint32_t sorted_count) : BaseNode(false) {
    header.size = sizeof(*this) + data_size;
    header.sorted_count = sorted_count;
  }
  ~InternalNode() = default;

  BaseNode *GetChild(char *key, uint64_t key_size);

 private:
  // Get the key (return value) and payload (8-byte)
  inline char *GetRecord(RecordMetadata meta, uint64_t &payload) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    char *data = &((char *) this + header.size)[meta.GetOffset()];
    payload = *(uint64_t *) (&data[meta.GetKeyLength()]);
    return data;
  }
};

class LeafNode : public BaseNode {
 public:
  static const uint32_t kNodeSize = 4096;

 public:
  static LeafNode *New();

  LeafNode() : BaseNode(true) {}
  ~LeafNode() = default;

  bool Insert(uint32_t epoch, char *key, uint32_t key_size, uint64_t payload,
              pmwcas::DescriptorPool *pmwcas_pool);

  // Consolidate all records in sorted order
  LeafNode *Consolidate(pmwcas::DescriptorPool *pmwcas_pool);

  // Get the key (return value) and payload (8-byte)
  inline char *GetRecord(RecordMetadata meta, uint64_t &payload) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    char *data = &((char *) this)[meta.GetOffset()];
    payload = *(uint64_t *) (&data[meta.GetKeyLength()]);
    return data;
  }

  inline char *GetKey(RecordMetadata meta) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    return &((char *) this)[meta.GetOffset()];
  }

  bool Delete(const char *key, uint32_t key_size, pmwcas::DescriptorPool *pmwcas_pool);

  void Dump();
 private:
  enum Uniqueness { IsUnique, Duplicate, ReCheck };
  Uniqueness CheckUnique(const char *key, uint32_t key_size);
  Uniqueness RecheckUnique(const char *key, uint32_t key_size, uint32_t end_pos);
  LeafNode::RecordMetadata *SearchRecord(const char *key,
                                         uint32_t key_size,
                                         uint32_t start_pos = 0,
                                         uint32_t end_pos = (uint32_t) -1);

};

class BzTree {
 private:
  struct Stack {
    static const uint32_t kMaxFrames = 32;
    InternalNode *frames[kMaxFrames];
    uint32_t num_frames;

    Stack() : num_frames(0) {}
    ~Stack() { num_frames = 0; }
    inline void Push(InternalNode *node) { frames[num_frames++] = node; }
    inline InternalNode *Pop() { return num_frames == 0 ? nullptr : frames[--num_frames]; }
    InternalNode *Top() { return num_frames == 0 ? nullptr : frames[num_frames - 1]; }
  };

 public:
  struct ParameterSet {
    uint32_t split_threshold;
    ParameterSet() : split_threshold(3072) {}
    ParameterSet(uint32_t split_threshold)
        : split_threshold(split_threshold) {}
    ~ParameterSet() {}
  };

  BzTree(ParameterSet param) : parameters(param), epoch(0), root(nullptr) {
    root = LeafNode::New();
  }
  bool Insert(char *key, uint64_t key_size);

 private:
  LeafNode *TraverseToLeaf(Stack &stack, char *key, uint64_t key_size);

 private:
  ParameterSet parameters;
  uint32_t epoch;
  BaseNode *root;
};

}  // namespace bztree
