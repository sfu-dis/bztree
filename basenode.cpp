// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include "basenode.h"

void BaseNode::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << " Dumping node: " << this << (is_leaf ? " (leaf)" : " (internal)") << std::endl;
  std::cout << " Header:\n";
  if (is_leaf) {
    std::cout << " - free space: " << GetFreeSpace()
              << std::endl;
  }
  std::cout << " - status: 0x" << std::hex << header.status.word << std::endl
            << "   (control = 0x" << (header.status.word & NodeHeader::StatusWord::kControlMask)
            << std::dec
            << ", frozen = " << header.status.IsFrozen()
            << ", block size = " << header.status.GetBlockSize()
            << ", delete size = " << header.status.GetDeletedSize()
            << ", record count = " << header.status.GetRecordCount() << ")\n"
            << " - sorted_count: " << header.sorted_count
            << std::endl;

  std::cout << " - size: " << header.size << std::endl;

  std::cout << " Record Metadata Array:" << std::endl;
  uint32_t n_meta = std::max<uint32_t>(header.status.GetRecordCount(), header.sorted_count);
  for (uint32_t i = 0; i < n_meta; ++i) {
    RecordMetadata meta = record_metadata[i];
    std::cout << " - record " << i << ": meta = 0x" << std::hex << meta.meta << std::endl;
    std::cout << std::hex;
    std::cout << "   (control = 0x" << (meta.meta & RecordMetadata::kControlMask)
              << std::dec
              << ", visible = " << meta.IsVisible()
              << ", offset = " << meta.GetOffset()
              << ", key length = " << meta.GetKeyLength()
              << ", total length = " << meta.GetTotalLength()
              << std::endl;
  }
}

bool BaseNode::GetRawRecord(RecordMetadata meta,
                            char **data,
                            char **key,
                            uint64_t *payload,
                            pmwcas::EpochManager *epoch) {
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

RecordMetadata BaseNode::SearchRecordMeta(pmwcas::EpochManager *epoch,
                                          const char *key,
                                          uint32_t key_size,
                                          RecordMetadata **out_metadata_ptr,
                                          uint32_t start_pos,
                                          uint32_t end_pos,
                                          bool check_concurrency) {
  if (start_pos < header.sorted_count) {
    // Binary search on sorted field
    int64_t first = start_pos;
    int64_t last = std::min<uint32_t>(end_pos, header.sorted_count - 1);
    int64_t middle;
    while (header.sorted_count != 0 && first <= last) {
      middle = (first + last) / 2;

      RecordMetadata current = GetMetadata(static_cast<uint32_t>(middle));

      char *current_key = nullptr;
      GetRawRecord(current, nullptr, &current_key, nullptr, epoch);
      assert(current_key || !is_leaf);

      auto cmp_result = KeyCompare(key, key_size, current_key, current.GetKeyLength());
      if (cmp_result < 0) {
        last = middle - 1;
      } else if (cmp_result == 0) {
        if (!current.IsVisible()) {
          break;
        }
        if (out_metadata_ptr) {
          *out_metadata_ptr = record_metadata + middle;
        }
        return current;
      } else {
        first = middle + 1;
      }
    }
  }
  if (end_pos > header.sorted_count) {
    // Linear search on unsorted field
    uint32_t linear_end = std::min<uint32_t>(header.GetStatus().GetRecordCount(), end_pos);
    for (uint32_t i = header.sorted_count; i < linear_end; i++) {
      RecordMetadata current = GetMetadata(i);

      if (current.IsInserting()) {
        if (check_concurrency) {
          // Encountered an in-progress insert, recheck later
          if (out_metadata_ptr) {
            *out_metadata_ptr = record_metadata + i;
          }
          return current;
        } else {
          continue;
        }
      }

      if (current.IsVisible()) {
        auto current_size = current.GetKeyLength();
        if (current_size == key_size &&
            KeyCompare(key, key_size, GetKey(current), current_size) == 0) {
          if (out_metadata_ptr) {
            *out_metadata_ptr = record_metadata + i;
          }
          return current;
        }
      }
    }
  }
  return RecordMetadata{0};
}

bool BaseNode::Freeze(nv_ptr<pmwcas::DescriptorPool> pmwcas_pool) {
  NodeHeader::StatusWord expected = header.GetStatus();
  if (expected.IsFrozen()) {
    return false;
  }

  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&header.status)->word, expected.word, expected.Freeze().word);
  return pd->MwCAS();
}
