// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <algorithm>
#include <iostream>
#include <string>

#include "bztree.h"

namespace bztree {

// Create an internal node with a new key and associated child pointers inserted
// based on an existing internal node
InternalNode *InternalNode::New(InternalNode *src_node,
                                const char *key,
                                uint32_t key_size,
                                uint64_t left_child_addr,
                                uint64_t right_child_addr) {
  // FIXME(tzwang): use a better allocator
  uint32_t alloc_size = src_node->GetHeader()->size +
                        RecordMetadata::PadKeyLength(key_size) +
                        sizeof(right_child_addr) + sizeof(RecordMetadata);
  InternalNode *node = reinterpret_cast<InternalNode *>(malloc(alloc_size));
  memset(node, 0, alloc_size);
  new (node) InternalNode(alloc_size, src_node, 0, src_node->header.sorted_count,
                          key, key_size, left_child_addr, right_child_addr);
  return node;
}

// Create an internal node with a single separator key and two pointers
InternalNode *InternalNode::New(const char *key,
                                uint32_t key_size,
                                uint64_t left_child_addr,
                                uint64_t right_child_addr) {
  uint32_t alloc_size = sizeof(InternalNode) +
                        RecordMetadata::PadKeyLength(key_size) +
                        sizeof(left_child_addr) +
                        sizeof(right_child_addr) +
                        sizeof(RecordMetadata) * 2;
  InternalNode *node = reinterpret_cast<InternalNode *>(malloc(alloc_size));
  memset(node, 0, alloc_size);
  new (node) InternalNode(alloc_size, key, key_size, left_child_addr, right_child_addr);
  return node;
}

// Create an internal node with keys and pointers in the provided range from an
// existing source node
InternalNode *InternalNode::New(InternalNode *src_node,
                                uint32_t begin_meta_idx, uint32_t nr_records,
                                const char *key, uint32_t key_size,
                                uint64_t left_child_addr, uint64_t right_child_addr,
                                uint64_t left_most_child_addr) {
  // Figure out how large the new node will be
  uint32_t alloc_size = sizeof(InternalNode);
  if (begin_meta_idx > 0) {
    // Will not copy from the first element (dummy key), so add it here
    alloc_size += (sizeof(uint64_t) + sizeof(RecordMetadata));
  }

  for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
    RecordMetadata meta = src_node->GetMetadata(i);
    alloc_size += meta.GetTotalLength();
    alloc_size += sizeof(RecordMetadata);
  }

  // Add the new key, if provided
  if (key) {
    LOG_IF(FATAL, key_size == 0);
    alloc_size +=
      (RecordMetadata::PadKeyLength(key_size) + sizeof(uint64_t) + sizeof(RecordMetadata));
  }

  InternalNode *node = reinterpret_cast<InternalNode *>(malloc(alloc_size));
  memset(node, 0, alloc_size);
  new (node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                          key, key_size, left_child_addr, right_child_addr, left_most_child_addr);
  return node;
}

InternalNode::InternalNode(uint32_t node_size,
                           const char *key,
                           const uint16_t key_size,
                           uint64_t left_child_addr,
                           uint64_t right_child_addr)
    : BaseNode(false, node_size) {
  // Initialize a new internal node with one key only
  header.sorted_count = 2;  // Includes the null dummy key

  // Fill in left child address, with an empty key
  uint64_t offset = node_size - sizeof(left_child_addr);
  record_metadata[0].FinalizeForInsert(offset, 0, sizeof(left_child_addr));
  char *ptr = reinterpret_cast<char *>(this) + offset;
  memcpy(ptr, &left_child_addr, sizeof(left_child_addr));

  // Fill in right child address, with the separator key
  auto padded_key_size = RecordMetadata::PadKeyLength(key_size);
  auto total_len = padded_key_size + sizeof(right_child_addr);
  offset -= total_len;
  record_metadata[1].FinalizeForInsert(offset, key_size, total_len);
  ptr = reinterpret_cast<char *>(this) + offset;
  memcpy(ptr, key, key_size);
  memcpy(ptr + padded_key_size, &right_child_addr, sizeof(right_child_addr));

  assert((uint64_t)ptr == (uint64_t)this + sizeof(*this) + 2 * sizeof(RecordMetadata));
}

InternalNode::InternalNode(uint32_t node_size,
                           InternalNode *src_node,
                           uint32_t begin_meta_idx,
                           uint32_t nr_records,
                           const char *key,
                           const uint16_t key_size,
                           uint64_t left_child_addr,
                           uint64_t right_child_addr,
                           uint64_t left_most_child_addr)
    : BaseNode(false, node_size) {
  LOG_IF(FATAL, !src_node);
  auto padded_key_size = RecordMetadata::PadKeyLength(key_size);

  uint64_t offset = node_size;
  bool need_insert_new = key;
  uint32_t insert_idx = 0;

  // See if we need a new left_most_child_addr, i.e., this must be the new node
  // on the right
  if (left_most_child_addr) {
    offset -= sizeof(uint64_t);
    record_metadata[0].FinalizeForInsert(offset, 0, sizeof(uint64_t));
    memcpy(reinterpret_cast<char *>(this) + offset, &left_most_child_addr, sizeof(uint64_t));
    ++insert_idx;
  }

  for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
    RecordMetadata meta = src_node->GetMetadata(i);
    uint64_t m_payload = 0;
    char *m_key = nullptr;
    char *m_data = nullptr;
    src_node->GetRecord(meta, &m_data, &m_key, &m_payload);
    auto m_key_size = meta.GetKeyLength();

    if (!need_insert_new) {
      // New key already inserted, so directly insert the key from src node
      assert(meta.GetTotalLength() >= sizeof(uint64_t));
      offset -= (meta.GetTotalLength());
      record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, meta.GetTotalLength());
      memcpy(reinterpret_cast<char *>(this) + offset, m_data, meta.GetTotalLength());
    } else {
      // Compare the two keys to see which one to insert (first)
      int cmp = memcmp(m_key, key, std::min<uint16_t>(m_key_size, key_size));
      LOG_IF(FATAL, cmp == 0 && key_size == m_key_size);

      if (cmp > 0) {
        assert(insert_idx >= 1);

        // Modify the previous key's payload to left_child_addr
        auto &prev_meta = record_metadata[insert_idx - 1];
        memcpy(reinterpret_cast<char *>(this) +
                   prev_meta.GetOffset() + prev_meta.GetPaddedKeyLength(),
               &left_child_addr, sizeof(left_child_addr));

        // Now the new separtor key itself
        offset -= (padded_key_size + sizeof(right_child_addr));
        record_metadata[insert_idx].FinalizeForInsert(
          offset, key_size, padded_key_size + sizeof(left_child_addr));

        ++insert_idx;
        memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
        memcpy(reinterpret_cast<char *>(this) + offset + padded_key_size,
               &right_child_addr, sizeof(right_child_addr));

        offset -= (meta.GetTotalLength());
        assert(meta.GetTotalLength() >= sizeof(uint64_t));
        record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, meta.GetTotalLength());
        memcpy(reinterpret_cast<char *>(this) + offset, m_data, meta.GetTotalLength());

        need_insert_new = false;
      } else {
        assert(meta.GetTotalLength() >= sizeof(uint64_t));
        offset -= (meta.GetTotalLength());
        record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, meta.GetTotalLength());
        memcpy(reinterpret_cast<char *>(this) + offset, m_data, meta.GetTotalLength());
      }
    }
    ++insert_idx;
  }

  if (need_insert_new) {
    // The new key-payload pair will be the right-most (largest key) element
    uint32_t total_size = RecordMetadata::PadKeyLength(key_size) + sizeof(uint64_t);
    offset -= total_size;
    record_metadata[insert_idx].FinalizeForInsert(offset, key_size, total_size);
    memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
    memcpy(reinterpret_cast<char *>(this) + offset + RecordMetadata::PadKeyLength(key_size),
           &right_child_addr, sizeof(right_child_addr));

    // Modify the previous key's payload to left_child_addr
    auto &prev_meta = record_metadata[insert_idx - 1];
    memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() + prev_meta.GetPaddedKeyLength(),
           &left_child_addr, sizeof(left_child_addr));

    ++insert_idx;
  }

  header.sorted_count = insert_idx;
}

InternalNode *InternalNode::PrepareForSplit(Stack &stack,
                                            uint32_t split_threshold,
                                            const char *key,
                                            uint32_t key_size,
                                            uint64_t left_child_addr,
                                            uint64_t right_child_addr) {
  uint32_t data_size = header.size + key_size +
      sizeof(right_child_addr) + sizeof(RecordMetadata);
  uint32_t new_node_size = sizeof(InternalNode) + data_size;
  if (new_node_size > split_threshold) {
    // After adding a key and pointers the new node would be too large. This
    // means we are effectively 'moving up' the tree to do split
    // So now we split the node and generate two new internal nodes
    auto status = header.status;
    if (status.IsFrozen()) {
      // Maybe hit another SMO, the entire split op will abort
      return nullptr;
    }

    LOG_IF(FATAL, header.sorted_count < 2);
    uint32_t n_left = header.sorted_count >> 1;

    InternalNode *left = nullptr;
    InternalNode *right = nullptr;

    // Figure out where does the new key will go
    auto separator_meta = GetMetadata(n_left);
    char *separator_key = nullptr;
    uint32_t separator_key_size = separator_meta.GetKeyLength();
    uint64_t separator_payload = 0;
    char *unused = nullptr;
    bool success = GetRecord(separator_meta, &unused, &separator_key, &separator_payload);
    LOG_IF(FATAL, !success);

    int cmp = memcmp(key, separator_key, std::min<uint32_t>(key_size, separator_key_size));
    if (cmp == 0) {
      cmp = key_size - separator_key_size;
    }
    LOG_IF(FATAL, cmp == 0);
    if (cmp < 0) {
      // Should go to left
      left = InternalNode::New(this, 0, n_left, key, key_size, left_child_addr, right_child_addr);
      right = InternalNode::New(this, n_left + 1, header.sorted_count - n_left - 1,
                                nullptr, 0, 0, 0, separator_payload);
    } else {
      left = InternalNode::New(this, 0, n_left, nullptr, 0, 0, 0);
      right = InternalNode::New(this, n_left + 1, header.sorted_count - n_left - 1,
                                key, key_size, left_child_addr, right_child_addr,
                                separator_payload);
    }
    assert(left);
    assert(right);

    // Pop here as if this were a leaf node so that when we get back to the
    // original caller, we get stack top as the "parent"
    stack.Pop();

    // Now get this internal node's real parent
    InternalNode *parent = stack.Top() ?
                           reinterpret_cast<InternalNode *>(stack.Pop()->node) : nullptr;
    if (parent) {
      // Need to insert into this parent, so create a new one
      return parent->PrepareForSplit(stack, split_threshold,
                                     separator_key, separator_key_size,
                                     (uint64_t)left, (uint64_t)right);
    } else {
      // New root node
      return InternalNode::New(separator_key, separator_key_size, (uint64_t)left, (uint64_t)right);
    }
  } else {
    return InternalNode::New(this, key, key_size, left_child_addr, right_child_addr);
  }
}

LeafNode *LeafNode::New() {
  // FIXME(tzwang): use a better allocator
  LeafNode *node = reinterpret_cast<LeafNode *>(malloc(kNodeSize));
  memset(node, 0, kNodeSize);
  new (node) LeafNode;
  return node;
}

void BaseNode::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << " Dumping node: " << this << (is_leaf ? " (leaf)" : " (internal)") << std::endl;
  std::cout << " Header:\n";
  if (is_leaf) {
    std::cout << " - free space: " << (reinterpret_cast<LeafNode *>(this))->GetFreeSpace()
              << std::endl;
  }
  std::cout << " - status: 0x" << std::hex << header.status.word << std::endl
            << "   (control = 0x" << (header.status.word & NodeHeader::StatusWord::kControlMask)
            << std::dec
            << ", frozen = " << header.status.IsFrozen()
            << ", block size = " << header.status.GetBlockSize()
            << ", delete size = " << header.status.GetDeleteSize()
            << ", record count = " << header.status.GetRecordCount() << ")\n"
            << " - sorted_count: " << header.sorted_count
            << std::endl;

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

void LeafNode::Dump() {
  BaseNode::Dump();
  std::cout << " Key-Payload Pairs:" << std::endl;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    RecordMetadata meta = record_metadata[i];
    uint64_t payload = 0;
    char *key = nullptr;
    GetRecord(meta, &key, &payload);
    std::string keystr(key, key + meta.GetKeyLength());
    std::cout << " - record " << i << ": key = " << keystr
              << ", payload = " << payload << std::endl;
  }

  std::cout << "-----------------------------" << std::endl;
}

void InternalNode::Dump(bool dump_children) {
  BaseNode::Dump();
  std::cout << " Child pointers and separator keys:" << std::endl;
  assert(header.status.GetRecordCount() == 0);
  for (uint32_t i = 0; i < header.sorted_count; ++i) {
    auto &meta = record_metadata[i];
    assert((i == 0 && meta.GetKeyLength() == 0) || (i > 0 && meta.GetKeyLength() > 0));
    uint64_t right_child_addr = 0;
    char *key = nullptr;
    char *unused = nullptr;
    GetRecord(meta, &unused, &key, &right_child_addr);
    if (key) {
      std::string keystr(key, key + meta.GetKeyLength());
      std::cout << " | " << keystr << " | ";
    }
    std::cout << std::hex << "0x" << right_child_addr << std::dec;
  }
  std::cout << std::endl;

  if (dump_children) {
    for (uint32_t i = 0; i < header.sorted_count; ++i) {
      uint64_t node_addr = *GetPayloadPtr(record_metadata[i]);
      BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
      if (node->IsLeaf()) {
        (reinterpret_cast<LeafNode *>(node_addr))->Dump();
      } else {
        (reinterpret_cast<InternalNode *>(node_addr))->Dump(true);
      }
    }
  }
}

ReturnCode LeafNode::Insert(uint32_t epoch, const char *key, uint16_t key_size, uint64_t payload,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  NodeHeader::StatusWord expected_status = header.status;

  // If frozon then retry
  if (expected_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  auto uniqueness = CheckUnique(key, key_size);
  if (uniqueness == Duplicate) {
    return ReturnCode::KeyExists();
  }

  // Now try to reserve space in the free space region using a PMwCAS. Two steps:
  // Step 1. Incrementing the record count and block size fields in [status]
  // Step 2. Flip the record metadata entry's high order bit and fill in global
  // epoch
  NodeHeader::StatusWord desired_status = expected_status;

  // Block size includes both key and payload sizes
  auto padded_key_size = RecordMetadata::PadKeyLength(key_size);
  auto total_size = padded_key_size + sizeof(payload);
  desired_status.PrepareForInsert(total_size);

  // Get the tentative metadata entry (again, make a local copy to work on it)
  RecordMetadata *meta_ptr = &record_metadata[expected_status.GetRecordCount()];
  RecordMetadata expected_meta = *meta_ptr;
  if (!expected_meta.IsVacant()) {
    goto retry;
  }

  RecordMetadata desired_meta;
  desired_meta.PrepareForInsert(epoch);

  // Now do the PMwCAS
  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&header.status.word, expected_status.word, desired_status.word);
  pd->AddEntry(&meta_ptr->meta, expected_meta.meta, desired_meta.meta);
  if (!pd->MwCAS()) {
    return ReturnCode::PMWCASFailure();
  }

  // Reserved space! Now copy data
  // The key size must be padded to 64bit
  uint64_t offset = kNodeSize - desired_status.GetBlockSize();
  char *ptr = &(reinterpret_cast<char *>(this))[offset];
  memcpy(ptr, key, key_size);
  memcpy(ptr + padded_key_size, &payload, sizeof(payload));
  // Flush the word
  pmwcas::NVRAM::Flush(total_size, ptr);

  if (uniqueness == ReCheck) {
    uniqueness = RecheckUnique(key, padded_key_size, expected_status.GetRecordCount());
    if (uniqueness == Duplicate) {
      memset(ptr, 0, key_size);
      memset(ptr + padded_key_size, 0, sizeof(payload));
      offset = 0;
    }
  }

  // Re-check if the node is frozen
  NodeHeader::StatusWord s = header.status;
  if (s.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  } else {
    // Final step: make the new record visible, a 2-word PMwCAS:
    // 1. Metadata - set the visible bit and actual block offset
    // 2. Status word - set to the initial value read above (s) to detect
    // conflicting threads that are trying to set the frozen bit
    expected_meta = desired_meta;
    desired_meta.FinalizeForInsert(offset, key_size, total_size);

    pd = pmwcas_pool->AllocateDescriptor();
    pd->AddEntry(&header.status.word, s.word, s.word);
    pd->AddEntry(&meta_ptr->meta, expected_meta.meta, desired_meta.meta);
    return pd->MwCAS() ? ReturnCode::Ok() : ReturnCode::PMWCASFailure();
  }
}

LeafNode::Uniqueness LeafNode::CheckUnique(const char *key, uint32_t key_size) {
  auto record = SearchRecordMeta(key, key_size);
  if (record == nullptr) {
    return IsUnique;
  }
  if (!record->IsVisible()) {
    return ReCheck;
  }
  return Duplicate;
}

LeafNode::Uniqueness LeafNode::RecheckUnique(const char *key, uint32_t key_size, uint32_t end_pos) {
  retry:
  auto record = SearchRecordMeta(key, key_size, header.sorted_count, end_pos);
  if (record == nullptr) {
    return IsUnique;
  }
  if (record->IsInserting()) {
    goto retry;
  }
  return Duplicate;
}

ReturnCode LeafNode::Upsert(uint32_t epoch,
                            const char *key,
                            uint16_t key_size,
                            uint64_t payload,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  auto old_status = header.status;
  if (old_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }
  auto *meta_ptr = SearchRecordMeta(key, key_size);
  if (meta_ptr == nullptr) {
    auto insert_result = Insert(epoch, key, key_size, payload, pmwcas_pool);

    if (insert_result.IsPMWCASFailure()) {
      return Update(epoch, key, key_size, payload, pmwcas_pool);
    }
    return ReturnCode::Ok();
  } else if (meta_ptr->IsInserting()) {
    goto retry;
  } else {
    return Update(epoch, key, key_size, payload, pmwcas_pool);
  }
}

ReturnCode LeafNode::Update(uint32_t epoch,
                            const char *key,
                            uint16_t key_size,
                            uint64_t payload,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  auto old_status = header.status;
  if (old_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  auto *meta_ptr = SearchRecordMeta(key, key_size);
  if (meta_ptr == nullptr || !meta_ptr->IsVisible()) {
    return ReturnCode::NotFound();
  } else if (meta_ptr->IsInserting()) {
    goto retry;
  }
  auto old_meta_value = meta_ptr->meta;

  char *record_key;
  uint64_t record_payload;
  GetRecord(*meta_ptr, &record_key, &record_payload);
  if (payload == record_payload) {
    return ReturnCode::Ok();
  }

//  1. update the corresponding payload
//  2. make sure meta data is not changed
//  3. make sure status word is not changed
  auto pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(reinterpret_cast<uint64_t *>(record_key + meta_ptr->GetPaddedKeyLength()),
               record_payload, payload);
  pd->AddEntry(&meta_ptr->meta, old_meta_value, meta_ptr->meta);
  pd->AddEntry(&header.status.word, old_status.word, old_status.word);

  if (!pd->MwCAS()) {
    goto retry;
  }
  return ReturnCode::Ok();
}

RecordMetadata *BaseNode::SearchRecordMeta(const char *key,
                                           uint32_t key_size,
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

      // Encountered a deleted record
      // Try to adjust the middle to left ones
      while (!record_metadata[middle].IsVisible() && first < middle) {
        middle -= 1;
      }

      // Every record on the left is deleted, now try right ones
      middle = (first + last) / 2;
      while (!record_metadata[middle].IsVisible() && middle < last) {
        middle += 1;
      }

      // Every record in the sorted field is deleted
      if (!record_metadata[middle].IsVisible()) {
        break;
      }

      uint64_t payload = 0;
      char *current_key = nullptr;
      char *unused = nullptr;
      auto current = &(record_metadata[middle]);
      GetRecord(*current, &unused, &current_key, &payload);

      auto cmp_result = memcmp(key, current_key, current->GetKeyLength());
      if (cmp_result < 0) {
        last = middle - 1;
      } else if (cmp_result == 0 && key_size == current->GetKeyLength() && current->IsVisible()) {
        return current;
      } else {
        first = middle + 1;
      }
    }
  }
  if (end_pos > header.sorted_count) {
    // Linear search on unsorted field
    uint32_t linear_end = std::min<uint32_t>(header.status.GetRecordCount(), end_pos);
    for (uint32_t i = header.sorted_count; i < linear_end; i++) {
      auto current = &(record_metadata[i]);

      // Encountered an in-progress insert, recheck later
      if (current->IsInserting() && check_concurrency) {
        return &(record_metadata[i]);
      } else if (current->IsInserting() && !check_concurrency) {
        continue;
      }

      uint64_t payload = 0;
      char *current_key = nullptr;
      char *unused = nullptr;
      GetRecord(*current, &unused, &current_key, &payload);
      if (current->IsVisible() &&
          key_size == current->GetKeyLength() &&
          strncmp(key, current_key, current->GetKeyLength()) == 0) {
        return current;
      }
    }
  }
  return nullptr;
}

ReturnCode LeafNode::Delete(const char *key,
                            uint16_t key_size,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  NodeHeader::StatusWord old_status = header.status;
  if (old_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  retry:
  auto record_meta = SearchRecordMeta(key, key_size);
  if (record_meta == nullptr) {
    return ReturnCode::NotFound();
  } else if (record_meta->IsInserting()) {
    // FIXME(hao): not mentioned in the paper, should confirm later;
    goto retry;
  }

  auto old_meta = *record_meta;
  auto new_meta = *record_meta;
  new_meta.SetVisible(false);
  new_meta.SetOffset(0);

  auto new_status = old_status;
  auto old_delete_size = old_status.GetDeleteSize();
  new_status.SetDeleteSize(old_delete_size + record_meta->GetTotalLength());

  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&header.status.word, old_status.word, new_status.word);
  pd->AddEntry(&(record_meta->meta), old_meta.meta, new_meta.meta);
  if (!pd->MwCAS()) {
    goto retry;
  }
  return ReturnCode::Ok();
}
ReturnCode LeafNode::Read(const char *key, uint16_t key_size, uint64_t *payload) {
  auto meta = SearchRecordMeta(key, key_size, 0, (uint32_t) -1, false);
  if (meta == nullptr) {
    return ReturnCode::NotFound();
  }
  char *unused_key;
  if (GetRecord(*meta, &unused_key, payload)) {
    return ReturnCode::Ok();
  } else {
    return ReturnCode::NotFound();
  }
}

bool BaseNode::Freeze(pmwcas::DescriptorPool *pmwcas_pool) {
  NodeHeader::StatusWord expected = header.status;
  if (expected.IsFrozen()) {
    return false;
  }
  NodeHeader::StatusWord desired = expected;
  desired.Freeze();

  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&header.status.word, expected.word, desired.word);
  return pd->MwCAS();
}

LeafNode *LeafNode::Consolidate(pmwcas::DescriptorPool *pmwcas_pool) {
  // Freeze the node to prevent new modifications first
  if (!Freeze(pmwcas_pool)) {
    return nullptr;
  }

  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  SortMetadataByKey(meta_vec, true);

  // Allocate and populate a new node
  LeafNode *new_leaf = LeafNode::New();
  new_leaf->CopyFrom(this, meta_vec.begin(), meta_vec.end());

  pmwcas::NVRAM::Flush(kNodeSize, new_leaf);

  return new_leaf;
}

uint32_t LeafNode::SortMetadataByKey(std::vector<RecordMetadata> &vec, bool visible_only) {
  uint32_t total_size = 0;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    // TODO(tzwang): handle deletes
    if (record_metadata[i].IsVisible()) {
      auto meta = record_metadata[i];
      vec.emplace_back(meta);
      total_size += (meta.GetTotalLength());
    }
  }

  // Lambda for comparing two keys
  auto key_cmp = [this](RecordMetadata &m1, RecordMetadata &m2) -> bool {
    uint64_t l1 = m1.GetKeyLength();
    uint64_t l2 = m2.GetKeyLength();
    char *k1 = GetKey(m1);
    char *k2 = GetKey(m2);
    int cmp = memcmp(k1, k2, std::min<uint64_t>(l1, l2));
    if (cmp == 0) {
      return l1 < l2;
    }
    return cmp < 0;
  };

  std::sort(vec.begin(), vec.end(), key_cmp);
  return total_size;
}

void LeafNode::CopyFrom(LeafNode *node,
                        std::vector<RecordMetadata>::iterator begin_it,
                        std::vector<RecordMetadata>::iterator end_it) {
  // meta_vec is assumed to be in sorted order, insert records one by one
  uint64_t offset = kNodeSize;
  uint16_t nrecords = 0;
  for (auto it = begin_it; it != end_it; ++it) {
    auto meta = *it;
    uint64_t payload = 0;
    char *key;
    node->GetRecord(meta, &key, &payload);

    // Copy data
    assert(meta.GetTotalLength() >= sizeof(uint64_t));
    uint64_t total_len = meta.GetTotalLength();
    offset -= total_len;
    char *ptr = &(reinterpret_cast<char *>(this))[offset];
    memcpy(ptr, key, total_len);

    // Setup new metadata
    record_metadata[nrecords].FinalizeForInsert(offset, meta.GetKeyLength(), total_len);
    ++nrecords;
  }
  // Finalize header stats
  header.status.SetBlockSize((uint32_t) (kNodeSize - offset));
  header.status.SetRecordCount(nrecords);
  header.sorted_count = nrecords;
}

ReturnCode InternalNode::Update(RecordMetadata meta,
                                InternalNode *old_child,
                                InternalNode *new_child,
                                pmwcas::DescriptorPool *pmwcas_pool) {
  auto status = header.status;
  if (status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  // Conduct a 2-word PMwCAS to swap in the new child pointer while ensuring the
  // node isn't frozen by a concurrent thread
  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&header.status.word, status.word, status.word);
  pd->AddEntry(GetPayloadPtr(meta),
               reinterpret_cast<uint64_t>(old_child),
               reinterpret_cast<uint64_t>(new_child));
  if (pd->MwCAS()) {
    return ReturnCode::Ok();
  } else {
    return ReturnCode::PMWCASFailure();
  }
}

BaseNode *InternalNode::GetChild(const char *key, uint16_t key_size, RecordMetadata *out_meta) {
  // Keys in internal nodes are always sorted, visible
  int32_t left = 0, right = header.sorted_count - 1, mid = 0;
  BaseNode *child_node = nullptr;
  RecordMetadata meta;
  while (true) {
    mid = (left + right) / 2;
    meta = record_metadata[mid];
    uint64_t meta_key_size = meta.GetKeyLength();
    uint64_t meta_payload = 0;
    char *meta_key = nullptr;
    char *unused = nullptr;
    GetRecord(meta, &unused, &meta_key, &meta_payload);
    int cmp = memcmp(key, meta_key, std::min<uint64_t>(meta_key_size, key_size));
    if (cmp == 0) {
      cmp = key_size - meta_key_size;
    }

    if (cmp == 0) {
      // Key exists
      assert(mid >= 1);
      meta = record_metadata[mid - 1];
      GetRecord(meta, &unused, &meta_key, &meta_payload);
      child_node = reinterpret_cast<BaseNode *>(meta_payload);
      break;
    } else {
      if (left > right) {
        if (cmp > 0) {
          child_node = reinterpret_cast<BaseNode *>(meta_payload);
        } else {
          assert(mid >= 1);
          meta = record_metadata[mid - 1];
          GetRecord(meta, &unused, &meta_key, &meta_payload);
          child_node = reinterpret_cast<BaseNode *>(meta_payload);
        }
        break;
      } else {
        if (cmp > 0) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
    }
  }

  if (out_meta) {
    *out_meta = meta;
  }
  assert(child_node);
  return child_node;
}

InternalNode *LeafNode::PrepareForSplit(uint32_t epoch, Stack &stack,
                                        uint32_t split_threshold,
                                        pmwcas::DescriptorPool *pmwcas_pool,
                                        LeafNode **left, LeafNode **right) {
  LOG_IF(FATAL, header.status.GetRecordCount() <= 2) << "Fewer than 2 records, can't split";
  // Set the frozen bit on the node to be split
  if (!Freeze(pmwcas_pool)) {
    return nullptr;
  }

  // Prepare new nodes: a parent node, a left leaf and a right leaf
  // FIXME(tzwang): not PM-safe, might leak
  *left = LeafNode::New();
  *right = LeafNode::New();

  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  uint32_t total_size = SortMetadataByKey(meta_vec, true);

  int32_t left_size = total_size / 2;
  uint32_t nleft = 0;
  for (uint32_t i = 0; i < meta_vec.size(); ++i) {
    auto &meta = meta_vec[i];
    ++nleft;
    left_size -= meta.GetTotalLength();
    if (left_size <= 0) {
      break;
    }
  }

  // TODO(tzwang): also put the new insert here to save some cycles
  auto left_end_it = meta_vec.begin() + nleft;
  (*left)->CopyFrom(this, meta_vec.begin(), left_end_it);
  (*right)->CopyFrom(this, left_end_it, meta_vec.end());

  // Separator exists in the new left leaf node, i.e., when traversing the tree,
  // we go left if <=, and go right if >.
  LOG_IF(FATAL, nleft - 1 == 0);
  RecordMetadata separator_meta = meta_vec[nleft - 1];

  InternalNode *parent = stack.Top() ?
                         reinterpret_cast<InternalNode *>(stack.Top()->node) : nullptr;

  // The node is already frozen (by us), so we must be able to get a valid key
  char *key = GetKey(separator_meta);
  assert(key);

  if (parent) {
    // Has a parent node. PrepareForSplit will see if we need to split this
    // parent node as well, and if so, return a new (possibly upper-level) parent
    // node that needs to be installed to its parent
    return parent->PrepareForSplit(stack, split_threshold, key, separator_meta.GetKeyLength(),
      reinterpret_cast<uint64_t>(*left), reinterpret_cast<uint64_t>(*right));
  } else {
    return InternalNode::New(key, separator_meta.GetKeyLength(),
      reinterpret_cast<uint64_t>(*left), reinterpret_cast<uint64_t>(*right));
  }
}

LeafNode *BzTree::TraverseToLeaf(Stack &stack, const char *key, uint64_t key_size) {
  BaseNode *node = root;
  InternalNode *parent = nullptr;
  assert(node);
  while (!node->IsLeaf()) {
    RecordMetadata meta;
    parent = reinterpret_cast<InternalNode *>(node);
    node = (reinterpret_cast<InternalNode *>(node))->GetChild(key, key_size, &meta);
    assert(node);
    stack.Push(parent, meta);
  }
  return reinterpret_cast<LeafNode *>(node);
}

ReturnCode BzTree::Insert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  ReturnCode rc;
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
  do {
    stack.Clear();
    LeafNode *node = TraverseToLeaf(stack, key, key_size);

    // Check space to see if we need to split the node
    auto new_node_size = node->GetUsedSpace() + sizeof(RecordMetadata) +
                         RecordMetadata::PadKeyLength(key_size) + sizeof(payload);
    if (new_node_size > parameters.split_threshold) {
      // Should split and we have three cases to handle:
      // 1. Root node is a leaf node - install [parent] as the new root
      // 2. We have a parent but no grandparent - install [parent] as the new
      //    root
      // 3. We have a grandparent - update the child pointer in the grandparent
      //    to point to the new [parent] (might further cause splits up the tree)

      // Note that when we split internal nodes (if needed), stack will get
      // Pop()'ed recursively, leaving the grantparent as the top (if any) here.
      // So we save the root node here in case we need to change root later.
      uint64_t old_root_addr = reinterpret_cast<uint64_t>(stack.GetRoot());
      if (!old_root_addr) {
        old_root_addr = reinterpret_cast<uint64_t>(node);
      }

      // Now split the leaf node. PrepareForSplit will return the node that we
      // need to install to the grandparent node (will be stack top, if any). If
      // it turns out there is no such grandparent, we directly install the
      // returned node as the new root.
      //
      // Note that in internal node's PrepareSplit if the internal node needs to
      // split we will pop the stack along the way as the split propogates
      // upward, such that by the time we come back here, the stack will contain
      // on its top the "parent" node and the "grandparent" node (if any) that
      // points to the parent node. As a result, we directly install a pointer to
      // the new parent node returned by leaf.PrepareForSplit to the grandparent.
      LeafNode *left = nullptr;
      LeafNode *right = nullptr;
      InternalNode *parent =
        node->PrepareForSplit(epoch, stack, parameters.split_threshold, pmwcas_pool, &left, &right);
      if (!parent) {
        // TODO(tzwang): check memory leaks
        continue;
      }

      auto *top = stack.Pop();
      InternalNode *old_parent = nullptr;
      if (top) {
        old_parent = top->node;
      }

      top = stack.Pop();
      InternalNode *gp = nullptr;
      if (top) {
        gp = reinterpret_cast<InternalNode *>(top->node);
      }

      if (gp) {
        assert(old_parent);
        // There is a grand parent. We need to swap out the pointer to the old
        // parent and install the pointer to the new parent. Don't care the
        // result here - have to retry anyway.
        gp->Update(top->meta, old_parent, parent, pmwcas_pool);
      } else {
        // No grand parent or already popped out by during split propogation
        if (!ChangeRoot(old_root_addr, parent)) {
            continue;
        }
      }
    } else {
      rc = node->Insert(epoch, key, key_size, payload, pmwcas_pool);
    }
  } while (!rc.IsOk() && !rc.IsKeyExists());
  return rc;
}

bool BzTree::ChangeRoot(uint64_t expected_root_addr, InternalNode *new_root) {
  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();

  // TODO(tzwang): specify memory policy for new leaf nodes
  pd->AddEntry(reinterpret_cast<uint64_t *>(&root),
               expected_root_addr,
               reinterpret_cast<uint64_t>(new_root));
  return pd->MwCAS();
}

ReturnCode BzTree::Read(const char *key, uint16_t key_size, uint64_t *payload) {
  thread_local Stack stack;
  stack.Clear();
  LeafNode *node = TraverseToLeaf(stack, key, key_size);
  if (node == nullptr) {
    return ReturnCode::NotFound();
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload);
  if (rc.IsOk()) {
    *payload = tmp_payload;
  }
  return rc;
}

ReturnCode BzTree::Update(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  ReturnCode rc;
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
  do {
    stack.Clear();
    LeafNode *node = TraverseToLeaf(stack, key, key_size);
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Update(epoch, key, key_size, payload, pmwcas_pool);
  } while (rc.IsPMWCASFailure());
  return rc;
}

ReturnCode BzTree::Upsert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.Clear();
  LeafNode *node = TraverseToLeaf(stack, key, key_size);
  if (node == nullptr) {
    return Insert(key, key_size, payload);
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload);
  if (rc.IsNotFound()) {
    return Insert(key, key_size, payload);
  } else if (rc.IsOk()) {
    if (tmp_payload == payload) {
      return ReturnCode::Ok();
    }
    return Update(key, key_size, payload);
  }
}

ReturnCode BzTree::Delete(const char *key, uint16_t key_size) {
  thread_local Stack stack;
  ReturnCode rc;
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
  do {
    stack.Clear();
    LeafNode *node = TraverseToLeaf(stack, key, key_size);
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Delete(key, key_size, pmwcas_pool);
    auto new_block_size = node->GetHeader()->status.GetBlockSize();
    if (new_block_size <= parameters.merge_threshold) {
      // FIXME(hao): merge the nodes
    }
  } while (rc.IsNodeFrozen());
  return rc;
}

void BzTree::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << "Dumping tree with root node: " << root << std::endl;
  // Traverse each level and dump each node
  if (root->IsLeaf()) {
    (reinterpret_cast<LeafNode *>(root))->Dump();
  } else {
    (reinterpret_cast<InternalNode *>(root))->Dump(true /* inlcude children */);
  }
}

}  // namespace bztree
