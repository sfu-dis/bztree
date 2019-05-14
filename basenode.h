// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include <cstdint>
#include <pmwcas.h>
#include <mwcas/mwcas.h>
#include "util.h"

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

    static const uint64_t kControlMask = uint64_t{0x7} << 61u;           // Bits 64-62
    static const uint64_t kFrozenMask = uint64_t{0x1} << 60u;            // Bit 61
    static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44u;    // Bits 60-45
    static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22u;    // Bits 44-23
    static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0u;    // Bits 22-1

    inline StatusWord Freeze() {
      return StatusWord{word | kFrozenMask};
    }
    inline bool IsFrozen() { return (word & kFrozenMask) > 0; }
    inline uint16_t GetRecordCount() { return (uint16_t)((word & kRecordCountMask) >> 44u); }
    inline void SetRecordCount(uint16_t count) {
      word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44u);
    }
    inline uint32_t GetBlockSize() { return (uint32_t)((word & kBlockSizeMask) >> 22u); }
    inline void SetBlockSize(uint32_t in_size) {
      word = (word & (~kBlockSizeMask)) | (uint64_t{in_size} << 22u);
    }
    inline uint32_t GetDeletedSize() { return (uint32_t)(word & kDeleteSizeMask); }
    inline void SetDeleteSize(uint32_t in_size) {
      word = (word & (~kDeleteSizeMask)) | uint64_t{in_size};
    }

    inline void PrepareForInsert(uint32_t in_size) {
      ALWAYS_ASSERT(in_size > 0);
      // Increment [record count] by one and [block size] by payload size
      word += ((uint64_t{1} << 44u) + (uint64_t{in_size} << 22u));
    }
  };

  StatusWord status;
  uint32_t size;
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

  static const uint64_t kControlMask = uint64_t{0x7} << 61u;           // Bits 64-62
  static const uint64_t kVisibleMask = uint64_t{0x1} << 60u;           // Bit 61
  static const uint64_t kOffsetMask = uint64_t{0xFFFFFFF} << 32u;      // Bits 60-33
  static const uint64_t kKeyLengthMask = uint64_t{0xFFFF} << 16u;      // Bits 32-17
  static const uint64_t kTotalLengthMask = uint64_t{0xFFFF};          // Bits 16-1

  static const uint64_t kAllocationEpochMask = uint64_t{0x7FFFFFF} << 32u;  // Bit 59-33

  inline bool IsVacant() { return meta == 0; }
  inline uint16_t GetKeyLength() const { return (uint16_t)((meta & kKeyLengthMask) >> 16u); }

  // Get the padded key length from accurate key length
  inline uint16_t GetPaddedKeyLength() {
    auto key_length = GetKeyLength();
    return PadKeyLength(key_length);
  }

  static inline constexpr uint16_t PadKeyLength(uint16_t key_length) {
    return (key_length + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
  }
  inline uint16_t GetTotalLength() { return (uint16_t)(meta & kTotalLengthMask); }
  inline uint32_t GetOffset() { return (uint32_t)((meta & kOffsetMask) >> 32u); }
  inline bool OffsetIsEpoch() {
    return (GetOffset() >> 27u) == 1;
  }
  inline void SetOffset(uint32_t offset) {
    meta = (meta & (~kOffsetMask)) | (uint64_t{offset} << 32u);
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
    assert(global_epoch < (uint64_t{1} << 27u));
    meta = (uint64_t{1} << 59u) | (global_epoch << 32u);
    assert(IsInserting());
  }
  inline void FinalizeForInsert(uint64_t offset, uint64_t key_len, uint64_t total_len) {
    // Set the actual offset, the visible bit, key/total length
    if (offset == 0) {
      // this record is duplicate inserted
      // make it invisible
      meta = (offset << 32u) | (uint64_t{0} << 60u) | (key_len << 16u) | total_len;
    } else {
      meta = (offset << 32u) | kVisibleMask | (key_len << 16u) | total_len;
    }
    assert(GetKeyLength() == key_len);
  }
  inline bool IsInserting() {
    // record is not visible
    // and record allocation epoch equal to global index epoch
    return !IsVisible() && OffsetIsEpoch() &&
        (((meta & kAllocationEpochMask) >> 32u) == global_epoch);
  }
};


class Stack;
class BaseNode {
 public:
  void Dump();

  inline uint32_t GetFreeSpace() {
    auto status = header.GetStatus();
    assert(header.size >= GetUsedSpace(status));
    return header.size - GetUsedSpace(status);
  }

  static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
    return sizeof(BaseNode) + status.GetBlockSize() +
        status.GetRecordCount() * sizeof(RecordMetadata);
  }

  // Set the frozen bit to prevent future modifications to the node
  bool Freeze(nv_ptr<pmwcas::DescriptorPool> pmwcas_pool);

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
                                  uint32_t end_pos = (uint32_t) - 1,
                                  bool check_concurrency = true);

  // Get the key and payload (8-byte), not thread-safe
  // Outputs:
  // 1. [*data] - pointer to the char string that stores key followed by payload.
  //    If the record has a null key, then this will point directly to the
  //    payload
  // 2. [*key] - pointer to the key (could be nullptr)
  // 3. [payload] - 8-byte payload
  bool GetRawRecord(RecordMetadata meta, char **data, char **key, uint64_t *payload,
                           pmwcas::EpochManager *epoch = nullptr);

  inline char *GetKey(RecordMetadata meta) {
    assert(meta.IsVisible());
    uint64_t offset = meta.GetOffset();
    return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
  }

  inline bool IsFrozen() {
    return GetHeader()->GetStatus().IsFrozen();
  }

  ReturnCode CheckMerge(Stack *stack, const char *key, uint32_t key_size, bool backoff);

 protected:
  bool is_leaf;
  NodeHeader header;
  RecordMetadata record_metadata[0];
};
