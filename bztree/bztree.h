#pragma once
#include "include/pmwcas.h"
#include "mwcas/mwcas.h"

namespace bztree {

struct NodeHeader {
  // Header:
  // |-------64 bits-------|---32 bits---|---32 bits---|
  // |     status word     |     size    | sorted count|
  //
  // Sorted count is actually the index into the next available metadata array
  // entry. Following the header is a growing array of record metadata entries.
  
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

    inline bool IsFrozen() { return word & kFrozenMask; }
    inline uint64_t GetRecordCount() { return (word & kRecordCountMask) >> 4; }
    inline uint64_t GetBlockSize() { return (word & kBlockSizeMask) >> 20; }
    inline uint64_t GetDeleteSize() { return (word & kDeleteSizeMask) >> 42; }
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
    inline uint64_t GetKeyLength() { return (meta & kKeyLengthMask) >> 32; }
    inline uint64_t GetTotalLength() { return (meta & kTotalLengthMask) >> 48; }
    inline uint64_t GetOffset() { return (meta & kOffsetMask) >> 4; }
    inline bool IsVisible() { return meta & kVisibleMask; }
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
  };

protected:
  NodeHeader header;
  RecordMetadata record_metadata[0];

public:
  BaseNode() {}
};

// Internal node: immutable once created, no free space, keys are sorted
class InternalNode : public BaseNode {
public:
  static const uint32_t kNodeSize = 4096;
  static InternalNode *New();

  InternalNode() : BaseNode() {}
  ~InternalNode() {}
};

class LeafNode : public BaseNode {
public:
  static const uint32_t kNodeSize = 4096;

public:
  static LeafNode *New();

  LeafNode() : BaseNode() {}
  ~LeafNode() {}

  bool Insert(uint32_t epoch, char *key, uint32_t key_size, uint64_t payload,
              pmwcas::DescriptorPool *pmwcas_pool);

  // Get the key (return value) and payload (8-byte)
  inline char *GetRecord(RecordMetadata meta, uint64_t &payload) {
    if (!meta.IsVisible()) {
      return nullptr;
    }
    uint64_t offset = meta.GetOffset();
    char *data = &((char*)this + kNodeSize)[meta.GetOffset()];
    payload = *(uint64_t*)(&data[meta.GetKeyLength()]);
    return data;
  }

  void Dump();
};

class BzTree {
public:
  BzTree() : epoch(0) {}
private:
  uint32_t epoch;
};

}  // namespace bztree
