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

namespace bztree {

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
}
