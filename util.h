// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include <cstdint>

extern uint64_t global_epoch;

template<typename T>
using nv_ptr= pmwcas::nv_ptr<T>;

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

static const inline int KeyCompare(const char *key1, uint32_t size1,
                                   const char *key2, uint32_t size2) {
  ALWAYS_ASSERT(key1 || key2);
  if (!key1) {
    return -1;
  } else if (!key2) {
    return 1;
  }

  auto cmp = memcmp(key1, key2, std::min<uint32_t>(size1, size2));
  if (cmp == 0) {
    return size1 - size2;
  }
  return cmp;
}

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
