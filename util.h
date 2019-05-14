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
