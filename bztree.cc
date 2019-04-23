// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#include <algorithm>
#include <iostream>
#include <string>

#include "bztree.h"

namespace bztree {

#ifdef PMDK
pmwcas::PMDKAllocator *Allocator::allocator_ = nullptr;
#endif

uint64_t global_epoch = 0;

void InternalNode::New(bztree::InternalNode **mem, uint32_t alloc_size) {
#ifdef  PMDK
  Allocator::Get()->AllocateDirect(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  (*mem)->header.size = alloc_size;
  *mem = Allocator::Get()->GetOffset(*mem);
#else
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  (*mem)->header.size = alloc_size;
#endif  // PMDK
}

// Create an internal node with a new key and associated child pointers inserted
// based on an existing internal node
void InternalNode::New(InternalNode *src_node,
                       const char *key,
                       uint32_t key_size,
                       uint64_t left_child_addr,
                       uint64_t right_child_addr,
                       InternalNode **mem) {
  uint32_t alloc_size = src_node->GetHeader()->size +
      RecordMetadata::PadKeyLength(key_size) +
      sizeof(right_child_addr) + sizeof(RecordMetadata);

#ifdef  PMDK
  Allocator::Get()->AllocateDirect(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  new(*mem) InternalNode(alloc_size, src_node, 0, src_node->header.sorted_count,
                         key, key_size, left_child_addr, right_child_addr);
  pmwcas::NVRAM::Flush(alloc_size, *mem);
  *mem = Allocator::Get()->GetOffset(*mem);
#else
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  new(*mem) InternalNode(alloc_size, src_node, 0, src_node->header.sorted_count,
                         key, key_size, left_child_addr, right_child_addr);
#ifdef PMEM
  pmwcas::NVRAM::Flush(alloc_size, *mem);
#endif  // PMEM
#endif  // PMDK
}

// Create an internal node with a single separator key and two pointers
void InternalNode::New(const char *key,
                       uint32_t key_size,
                       uint64_t left_child_addr,
                       uint64_t right_child_addr,
                       InternalNode **mem) {
  uint32_t alloc_size = sizeof(InternalNode) +
      RecordMetadata::PadKeyLength(key_size) +
      sizeof(left_child_addr) +
      sizeof(right_child_addr) +
      sizeof(RecordMetadata) * 2;
#ifdef PMDK
  Allocator::Get()->AllocateDirect(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  new(*mem) InternalNode(alloc_size, key, key_size, left_child_addr, right_child_addr);
  pmwcas::NVRAM::Flush(alloc_size, *mem);
  *mem = Allocator::Get()->GetOffset(*mem);
#else
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(mem), alloc_size);
  memset(*mem, 0, alloc_size);
  new(*mem) InternalNode(alloc_size, key, key_size, left_child_addr, right_child_addr);
#ifdef PMEM
  pmwcas::NVRAM::Flush(alloc_size, *mem);
#endif  // PMEM
#endif  // PMDK
}

// Create an internal node with keys and pointers in the provided range from an
// existing source node
void InternalNode::New(InternalNode *src_node,
                       uint32_t begin_meta_idx, uint32_t nr_records,
                       const char *key, uint32_t key_size,
                       uint64_t left_child_addr, uint64_t right_child_addr,
                       InternalNode **new_node,
                       uint64_t left_most_child_addr) {
  // Figure out how large the new node will be
  uint32_t alloc_size = sizeof(InternalNode);
  if (begin_meta_idx > 0) {
    // Will not copy from the first element (dummy key), so add it here
    alloc_size += src_node->record_metadata[0].GetTotalLength();
    alloc_size += sizeof(RecordMetadata);
  }

  assert(nr_records > 0);
  for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
    RecordMetadata meta = src_node->record_metadata[i];
    alloc_size += meta.GetTotalLength();
    alloc_size += sizeof(RecordMetadata);
  }

  // Add the new key, if provided
  if (key) {
    ALWAYS_ASSERT(key_size > 0);
    alloc_size +=
        (RecordMetadata::PadKeyLength(key_size) + sizeof(uint64_t) + sizeof(RecordMetadata));
  }

#ifdef PMDK
  Allocator::Get()->AllocateDirect(reinterpret_cast<void **>(new_node), alloc_size);
  memset(*new_node, 0, alloc_size);
  new(*new_node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                              key, key_size, left_child_addr, right_child_addr,
                              left_most_child_addr);
  pmwcas::NVRAM::Flush(alloc_size, new_node);
  *new_node = Allocator::Get()->GetOffset(*new_node);
#else
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(new_node), alloc_size);
  memset(*new_node, 0, alloc_size);
  new(*new_node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                              key, key_size, left_child_addr, right_child_addr,
                              left_most_child_addr);
#ifdef PMEM
  pmwcas::NVRAM::Flush(alloc_size, *new_node);
#endif  // PMEM
#endif  // PMDK
}

InternalNode::InternalNode(uint32_t node_size,
                           const char *key,
                           const uint16_t key_size,
                           uint64_t left_child_addr,
                           uint64_t right_child_addr)
    : BaseNode(false, node_size) {
  // Initialize a new internal node with one key only
  header.sorted_count = 2;  // Includes the null dummy key
  header.size = node_size;

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

  assert((uint64_t) ptr == (uint64_t) this + sizeof(*this) + 2 * sizeof(RecordMetadata));
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
  ALWAYS_ASSERT(src_node);
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

  assert(nr_records > 0);

  for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
    RecordMetadata meta = src_node->record_metadata[i];
    assert(meta.IsVisible());
    uint64_t m_payload = 0;
    char *m_key = nullptr;
    char *m_data = nullptr;
    src_node->GetRawRecord(meta, &m_data, &m_key, &m_payload);
    auto m_key_size = meta.GetKeyLength();

    if (!need_insert_new) {
      // New key already inserted, so directly insert the key from src node
      assert(meta.GetTotalLength() >= sizeof(uint64_t));
      offset -= (meta.GetTotalLength());
      record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, meta.GetTotalLength());
      memcpy(reinterpret_cast<char *>(this) + offset, m_data, meta.GetTotalLength());
    } else {
      // Compare the two keys to see which one to insert (first)
      auto cmp = KeyCompare(m_key, m_key_size, key, key_size);
      ALWAYS_ASSERT(!(cmp == 0 && key_size == m_key_size));

      if (cmp > 0) {
        assert(insert_idx >= 1);

        // Modify the previous key's payload to left_child_addr
        auto prev_meta = record_metadata[insert_idx - 1];

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
    auto prev_meta = record_metadata[insert_idx - 1];
    memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() + prev_meta.GetPaddedKeyLength(),
           &left_child_addr, sizeof(left_child_addr));

    ++insert_idx;
  }

  header.size = node_size;
  header.sorted_count = insert_idx;
}

// Insert record to this internal node. The node is frozen at this time.
bool InternalNode::PrepareForSplit(Stack &stack,
                                   uint32_t split_threshold,
                                   const char *key,
                                   uint32_t key_size,
                                   uint64_t left_child_addr,   // [key]'s left child pointer
                                   uint64_t right_child_addr,  // [key]'s right child pointer
                                   InternalNode **new_node,
                                   pmwcas::Descriptor *pd,
                                   pmwcas::DescriptorPool *pool,
                                   bool backoff) {
  uint32_t data_size = header.size + key_size +
      sizeof(right_child_addr) + sizeof(RecordMetadata);
  uint32_t new_node_size = sizeof(InternalNode) + data_size;
  if (new_node_size < split_threshold) {
    // good boy
    InternalNode::New(this, key, key_size, left_child_addr,
                      right_child_addr, new_node);
    return true;
  }

  // After adding a key and pointers the new node would be too large. This
  // means we are effectively 'moving up' the tree to do split
  // So now we split the node and generate two new internal nodes
  ALWAYS_ASSERT(header.sorted_count >= 2);
  uint32_t n_left = header.sorted_count >> 1;

  auto i_left = pd->ReserveAndAddEntry(
      reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
      reinterpret_cast<uint64_t>(nullptr),
      pmwcas::Descriptor::kRecycleOnRecovery);
  auto i_right = pd->ReserveAndAddEntry(
      reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
      reinterpret_cast<uint64_t>(nullptr),
      pmwcas::Descriptor::kRecycleOnRecovery);
  uint64_t *ptr_l = pd->GetNewValuePtr(i_left);
  uint64_t *ptr_r = pd->GetNewValuePtr(i_right);

  // Figure out where the new key will go
  auto separator_meta = record_metadata[n_left];
  char *separator_key = nullptr;
  uint16_t separator_key_size = separator_meta.GetKeyLength();
  uint64_t separator_payload = 0;
  bool success = GetRawRecord(separator_meta, nullptr, &separator_key,
                              &separator_payload);
  ALWAYS_ASSERT(success);

  int cmp = memcmp(key, separator_key, std::min<uint32_t>(key_size, separator_key_size));
  if (cmp == 0) {
    cmp = key_size - separator_key_size;
  }
  ALWAYS_ASSERT(cmp != 0);
  if (cmp < 0) {
    // Should go to left
    InternalNode::New(this, 0, n_left, key, key_size,
                      left_child_addr, right_child_addr,
                      reinterpret_cast<InternalNode **>(ptr_l), 0);
    InternalNode::New(this, n_left + 1, header.sorted_count - n_left - 1,
                      nullptr, 0, 0, 0,
                      reinterpret_cast<InternalNode **>(ptr_r), separator_payload);
  } else {
    InternalNode::New(this, 0, n_left, nullptr, 0, 0, 0,
                      reinterpret_cast<InternalNode **>(ptr_l), 0);
    InternalNode::New(this, n_left + 1, header.sorted_count - n_left - 1,
                      key, key_size, left_child_addr, right_child_addr,
                      reinterpret_cast<InternalNode **>(ptr_r), separator_payload);
  }
  assert(*ptr_l);
  assert(*ptr_r);

  // Pop here as if this were a leaf node so that when we get back to the
  // original caller, we get stack top as the "parent"
  stack.Pop();

  // Now get this internal node's real parent
  InternalNode *parent = stack.Top() ?
                         stack.Top()->node : nullptr;
  if (parent == nullptr) {
    // Good!
    InternalNode::New(separator_key, separator_key_size,
                      (uint64_t) *ptr_l, (uint64_t) *ptr_r, new_node);
    return true;
  }

  // Try to freeze the parent node first
  bool frozen_by_me = false;
  while (!parent->IsFrozen(pool->GetEpoch())) {
    frozen_by_me = parent->Freeze(pool);
  }

  // Someone else froze the parent node and we are told not to compete with
  // others (for now)
  if (!frozen_by_me && backoff) {
    return false;
  }

  return parent->PrepareForSplit(stack, split_threshold,
                                 separator_key, separator_key_size,
                                 (uint64_t) *ptr_l, (uint64_t) *ptr_r,
                                 new_node, pd, pool, backoff);
}

void LeafNode::New(LeafNode **mem, uint32_t node_size) {
#ifdef PMDK
  Allocator::Get()->AllocateDirect(reinterpret_cast<void **>(mem), node_size);
  memset(*mem, 0, node_size);
  new(*mem)LeafNode(node_size);
  pmwcas::NVRAM::Flush(node_size, *mem);
  *mem = Allocator::Get()->GetOffset(*mem);
#else
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(mem), node_size);
  memset(*mem, 0, node_size);
  new(*mem) LeafNode(node_size);
#ifdef PMEM
  pmwcas::NVRAM::Flush(node_size, *mem);
#endif  // PMEM
#endif  // PMDK
}

void BaseNode::Dump(pmwcas::EpochManager *epoch) {
  std::cout << "-----------------------------" << std::endl;
  std::cout << " Dumping node: " << this << (is_leaf ? " (leaf)" : " (internal)") << std::endl;
  std::cout << " Header:\n";
  if (is_leaf) {
    std::cout << " - free space: " << (reinterpret_cast<LeafNode *>(this))->GetFreeSpace(epoch)
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

void LeafNode::Dump(pmwcas::EpochManager *epoch) {
  BaseNode::Dump(epoch);
  std::cout << " Key-Payload Pairs:" << std::endl;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    RecordMetadata meta = record_metadata[i];
    if (meta.IsVisible()) {
      uint64_t payload = 0;
      char *key = nullptr;
      GetRawRecord(meta, &key, &payload, epoch);
      assert(key);
      std::string keystr(key, key + meta.GetKeyLength());
      std::cout << " - record " << i << ": key = " << keystr
                << ", payload = " << payload << std::endl;
    }
  }

  std::cout << "-----------------------------" << std::endl;
}

void InternalNode::Dump(pmwcas::EpochManager *epoch, bool dump_children) {
  BaseNode::Dump(epoch);
  std::cout << " Child pointers and separator keys:" << std::endl;
  assert(header.status.GetRecordCount() == 0);
  for (uint32_t i = 0; i < header.sorted_count; ++i) {
    auto &meta = record_metadata[i];
    assert((i == 0 && meta.GetKeyLength() == 0) || (i > 0 && meta.GetKeyLength() > 0));
    uint64_t right_child_addr = 0;
    char *key = nullptr;
    GetRawRecord(meta, nullptr, &key, &right_child_addr);
    if (key) {
      std::string keystr(key, key + meta.GetKeyLength());
      std::cout << " || " << keystr << " | ";
    }
    std::cout << std::hex << "0x" << right_child_addr << std::dec;
  }
  std::cout << std::endl;

  if (dump_children) {
    for (uint32_t i = 0; i < header.sorted_count; ++i) {
      uint64_t node_addr = *GetPayloadPtr(record_metadata[i]);
#ifdef  PMDK
      BaseNode *node = Allocator::Get()->GetDirect(reinterpret_cast<BaseNode *>(node_addr));
#else
      BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
#endif
      if (node->IsLeaf()) {
        (reinterpret_cast<LeafNode *>(node))->Dump(epoch);
      } else {
        (reinterpret_cast<InternalNode *>(node))->Dump(epoch, true);
      }
    }
  }
}

ReturnCode LeafNode::Insert(const char *key, uint16_t key_size, uint64_t payload,
                            pmwcas::DescriptorPool *pmwcas_pool, uint32_t split_threshold) {
  retry:
  NodeHeader::StatusWord expected_status = header.GetStatus(pmwcas_pool->GetEpoch());

  // If frozon then retry
  if (expected_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  auto uniqueness = CheckUnique(key, key_size, pmwcas_pool->GetEpoch());
  if (uniqueness == Duplicate) {
    return ReturnCode::KeyExists();
  }

  // Check space to see if we need to split the node
  auto new_size = LeafNode::GetUsedSpace(expected_status) + sizeof(RecordMetadata) +
      RecordMetadata::PadKeyLength(key_size) + sizeof(payload);
  if (new_size >= split_threshold) {
    return ReturnCode::NotEnoughSpace();
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
  desired_meta.PrepareForInsert();

  // Now do the PMwCAS
  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&header.status)->word, expected_status.word, desired_status.word);
  pd->AddEntry(&meta_ptr->meta, expected_meta.meta, desired_meta.meta);
  if (!pd->MwCAS()) {
    goto retry;
  }

  // Reserved space! Now copy data
  // The key size must be padded to 64bit
  uint64_t offset = header.size - desired_status.GetBlockSize();
  char *ptr = &(reinterpret_cast<char *>(this))[offset];
  memcpy(ptr, key, key_size);
  memcpy(ptr + padded_key_size, &payload, sizeof(payload));
  // Flush the word

#ifdef PMEM
  pmwcas::NVRAM::Flush(total_size, ptr);
#endif

  retry_phase2:
  // Re-check if the node is frozen
  if (uniqueness == ReCheck) {
    auto new_uniqueness = RecheckUnique(key, key_size,
                                        expected_status.GetRecordCount(),
                                        pmwcas_pool->GetEpoch());
    if (new_uniqueness == Duplicate) {
      memset(ptr, 0, key_size);
      memset(ptr + padded_key_size, 0, sizeof(payload));
      offset = 0;
    } else if (new_uniqueness == NodeFrozen) {
      return ReturnCode::NodeFrozen();
    }
  }
  // Final step: make the new record visible, a 2-word PMwCAS:
  // 1. Metadata - set the visible bit and actual block offset
  // 2. Status word - set to the initial value read above (s) to detect
  // conflicting threads that are trying to set the frozen bit
  auto new_meta = desired_meta;
  new_meta.FinalizeForInsert(offset, key_size, total_size);
  assert(new_meta.GetTotalLength() < 100);

  NodeHeader::StatusWord s = header.GetStatus(pmwcas_pool->GetEpoch());
  if (s.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }
  pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&header.status)->word, s.word, s.word);
  pd->AddEntry(&meta_ptr->meta, desired_meta.meta, new_meta.meta);
  if (pd->MwCAS()) {
    return ReturnCode::Ok();
  } else {
    goto retry_phase2;
  }
}

LeafNode::Uniqueness LeafNode::CheckUnique(const char *key,
                                           uint32_t key_size,
                                           pmwcas::EpochManager *epoch) {
  auto metadata = SearchRecordMeta(epoch, key, key_size, nullptr);
  if (metadata.IsVacant()) {
    return IsUnique;
  }
  // we need to perform a key compare again
  // consider this case:
  // a key is inserting when we "SearchRecordMeta"
  // when get back, this meta may have finished inserting, so the following if will be false
  // however, this key may not be duplicate, so we need to compare the key again
  // even if this key is not duplicate, we need to return a "Recheck"
  if (metadata.IsInserting()) {
    return ReCheck;
  }
  assert(metadata.IsVisible());
  char *curr_key = GetKey(metadata);
  assert(curr_key);
  if (KeyCompare(key, key_size, curr_key, metadata.GetKeyLength()) == 0) {
    return Duplicate;
  }
  return ReCheck;
}

LeafNode::Uniqueness LeafNode::RecheckUnique(const char *key, uint32_t key_size,
                                             uint32_t end_pos, pmwcas::EpochManager *epoch) {
  retry:
  auto current_status = GetHeader()->GetStatus(epoch);
  if (current_status.IsFrozen()) {
    return NodeFrozen;
  }
  auto metadata = SearchRecordMeta(epoch, key, key_size, nullptr, header.sorted_count, end_pos);
  if (metadata.IsVacant()) {
    return IsUnique;
  }
  if (metadata.IsInserting()) {
    goto retry;
  }
  assert(metadata.IsVisible());
  char *curr_key = GetKey(metadata);
  if (KeyCompare(key, key_size, curr_key, metadata.GetKeyLength()) == 0) {
    return Duplicate;
  }
  return IsUnique;
}

ReturnCode LeafNode::Update(const char *key,
                            uint16_t key_size,
                            uint64_t payload,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  auto old_status = header.GetStatus(pmwcas_pool->GetEpoch());
  if (old_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  RecordMetadata *meta_ptr = nullptr;
  auto metadata = SearchRecordMeta(pmwcas_pool->GetEpoch(), key, key_size, &meta_ptr);
  if (metadata.IsVacant()) {
    return ReturnCode::NotFound();
  } else if (metadata.IsInserting()) {
    goto retry;
  }

  char *record_key = nullptr;
  uint64_t record_payload = 0;
  GetRawRecord(metadata, &record_key, &record_payload, pmwcas_pool->GetEpoch());
  if (payload == record_payload) {
    return ReturnCode::Ok();
  }

  // 1. Update the corresponding payload
  // 2. Make sure meta data is not changed
  // 3. Make sure status word is not changed
  auto pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(reinterpret_cast<uint64_t *>(record_key + metadata.GetPaddedKeyLength()),
               record_payload, payload);
  pd->AddEntry(&meta_ptr->meta, metadata.meta, metadata.meta);
  pd->AddEntry(&(&header.status)->word, old_status.word, old_status.word);

  if (!pd->MwCAS()) {
    goto retry;
  }
  return ReturnCode::Ok();
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

      RecordMetadata current = GetMetadata(static_cast<uint32_t>(middle), epoch);

      char *current_key = nullptr;
      GetRawRecord(current, nullptr, &current_key, nullptr, epoch);
      assert(current_key || !is_leaf);

      auto cmp_result = KeyCompare(key, key_size, current_key, current.GetKeyLength());
      if (cmp_result < 0) {
        last = middle - 1;
      } else if (cmp_result == 0) {
        // The found record isn't visible
        if (!current.IsVisible()) {
          return RecordMetadata{0};
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
    uint32_t linear_end = std::min<uint32_t>(header.GetStatus(epoch).GetRecordCount(), end_pos);
    for (uint32_t i = header.sorted_count; i < linear_end; i++) {
      RecordMetadata current = GetMetadata(i, epoch);

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
        char *current_key = nullptr;
        GetRawRecord(current, nullptr, &current_key, nullptr, epoch);
        assert(current_key || !is_leaf);
        if (KeyCompare(key, key_size, current_key, current.GetKeyLength()) == 0) {
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

ReturnCode LeafNode::Delete(const char *key,
                            uint16_t key_size,
                            pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  NodeHeader::StatusWord old_status = header.GetStatus(pmwcas_pool->GetEpoch());
  if (old_status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  RecordMetadata *meta_ptr = nullptr;
  auto metadata = SearchRecordMeta(pmwcas_pool->GetEpoch(), key, key_size, &meta_ptr);
  if (metadata.IsVacant()) {
    return ReturnCode::NotFound();
  } else if (metadata.IsInserting()) {
    // FIXME(hao): not mentioned in the paper, should confirm later;
    goto retry;
  }

  auto new_meta = metadata;
  new_meta.SetVisible(false);

  auto new_status = old_status;
  auto old_delete_size = old_status.GetDeletedSize();
  new_status.SetDeleteSize(old_delete_size + metadata.GetTotalLength());

  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&header.status)->word, old_status.word, new_status.word);
  pd->AddEntry(&meta_ptr->meta, metadata.meta, new_meta.meta);
  if (!pd->MwCAS()) {
    goto retry;
  }
  return ReturnCode::Ok();
}
ReturnCode LeafNode::Read(const char *key, uint16_t key_size, uint64_t *payload,
                          pmwcas::DescriptorPool *pmwcas_pool) {
  auto meta = SearchRecordMeta(pmwcas_pool->GetEpoch(), key, key_size, nullptr,
                               0, (uint32_t) -1, false);
  if (meta.IsVacant()) {
    return ReturnCode::NotFound();
  }

  char *source_addr = (reinterpret_cast<char *>(this) + meta.GetOffset());
  *payload = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
      source_addr + meta.GetPaddedKeyLength())->GetValue(pmwcas_pool->GetEpoch());
  return ReturnCode::Ok();
}

ReturnCode LeafNode::RangeScan(const char *key1,
                               uint32_t size1,
                               const char *key2,
                               uint32_t size2,
                               std::vector<Record *> *result,
                               pmwcas::DescriptorPool *pmwcas_pool) {
  // entering a new epoch and copying the data
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());

  // scan the sorted fields first
  uint32_t i = 0;
  auto count = header.GetStatus(pmwcas_pool->GetEpoch()).GetRecordCount();
  while (i < count) {
    auto curr_meta = GetMetadata(i, pmwcas_pool->GetEpoch());
    if (!curr_meta.IsVisible()) {
      i += 1;
      continue;
    }
    char *curr_key;
    GetRawRecord(curr_meta, &curr_key, nullptr, pmwcas_pool->GetEpoch());
    auto range_code = KeyInRange(curr_key, curr_meta.GetKeyLength(), key1, size1, key2, size2);
    if (range_code == 0) {
      result->emplace_back(Record::New(curr_meta, this, pmwcas_pool->GetEpoch()));
    } else if (range_code == 1 && i < header.sorted_count) {
      // current key is larger than upper bound
      // jump to the unsorted field
      i = header.sorted_count;
      continue;
    }
    i += 1;
  }
  std::sort(result->begin(), result->end(),
            [this](Record *a, Record *b) -> bool {
              auto cmp = BaseNode::KeyCompare(a->GetKey(), a->meta.GetKeyLength(),
                                              b->GetKey(), b->meta.GetKeyLength());
              return cmp < 0;
            });
  return ReturnCode::Ok();
}

bool BaseNode::Freeze(pmwcas::DescriptorPool *pmwcas_pool) {
  NodeHeader::StatusWord expected = header.GetStatus(pmwcas_pool->GetEpoch());
  if (expected.IsFrozen()) {
    return false;
  }

  pmwcas::Descriptor *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&header.status)->word, expected.word, expected.Freeze().word);
  return pd->MwCAS();
}

LeafNode *LeafNode::Consolidate(pmwcas::DescriptorPool *pmwcas_pool) {
  // Freeze the node to prevent new modifications first
  if (!Freeze(pmwcas_pool)) {
    return nullptr;
  }

  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  SortMetadataByKey(meta_vec, true, pmwcas_pool->GetEpoch());

  // Allocate and populate a new node
  LeafNode *new_leaf = nullptr;
  LeafNode::New(&new_leaf, this->header.size);
  new_leaf->CopyFrom(this, meta_vec.begin(), meta_vec.end(), pmwcas_pool->GetEpoch());

#ifdef PMEM
  pmwcas::NVRAM::Flush(this->header.size, new_leaf);
#endif

  return new_leaf;
}

uint32_t LeafNode::SortMetadataByKey(std::vector<RecordMetadata> &vec,
                                     bool visible_only,
                                     pmwcas::EpochManager *epoch) {
  // Node is frozen at this point
  // there should not be any on-going pmwcas
  assert(header.status.IsFrozen());
  uint32_t total_size = 0;
  uint32_t count = header.GetStatus(epoch).GetRecordCount();
  for (uint32_t i = 0; i < count; ++i) {
    // TODO(tzwang): handle deletes
    auto meta = record_metadata[i];
    if (meta.IsVisible()) {
      vec.emplace_back(meta);
      total_size += (meta.GetTotalLength());
      assert(meta.GetTotalLength());
    }
  }

  // Lambda for comparing two keys
  auto key_cmp = [this](RecordMetadata &m1, RecordMetadata &m2) -> bool {
    auto l1 = m1.GetKeyLength();
    auto l2 = m2.GetKeyLength();
    char *k1 = GetKey(m1);
    char *k2 = GetKey(m2);
    return KeyCompare(k1, l1, k2, l2) < 0;
  };

  std::sort(vec.begin(), vec.end(), key_cmp);
  return total_size;
}

void LeafNode::CopyFrom(LeafNode *node,
                        std::vector<RecordMetadata>::iterator begin_it,
                        std::vector<RecordMetadata>::iterator end_it,
                        pmwcas::EpochManager *epoch) {
  // meta_vec is assumed to be in sorted order, insert records one by one
  uint32_t offset = this->header.size;
  uint16_t nrecords = 0;
  for (auto it = begin_it; it != end_it; ++it) {
    auto meta = *it;
    uint64_t payload = 0;
    char *key;
    node->GetRawRecord(meta, &key, &payload, epoch);

    // Copy data
    assert(meta.GetTotalLength() >= sizeof(uint64_t));
    uint64_t total_len = meta.GetTotalLength();
    assert(offset >= total_len);
    offset -= total_len;
    char *ptr = &(reinterpret_cast<char *>(this))[offset];
    memcpy(ptr, key, total_len);

    // Setup new metadata
    record_metadata[nrecords].FinalizeForInsert(offset, meta.GetKeyLength(), total_len);
    ++nrecords;
  }
  // Finalize header stats
  header.status.SetBlockSize(this->header.size - offset);
  header.status.SetRecordCount(nrecords);
  header.sorted_count = nrecords;
#ifdef PMDK
  Allocator::Get()->PersistPtr(this, this->header.size);
#endif
}

void InternalNode::DeleteRecord(uint32_t meta_to_update,
                                uint64_t new_child_ptr,
                                bztree::InternalNode **new_node) {
  uint32_t meta_to_delete = meta_to_update + 1;
  uint32_t offset = this->header.size -
      this->record_metadata[meta_to_delete].GetTotalLength() - sizeof(RecordMetadata);
  InternalNode::New(new_node, offset);

  uint32_t insert_idx = 0;
  for (uint32_t i = 0; i < this->header.sorted_count; i += 1) {
    if (i == meta_to_delete) {
      continue;
    }
    RecordMetadata meta = record_metadata[i];
    uint64_t m_payload = 0;
    char *m_key = nullptr;
    char *m_data = nullptr;
    GetRawRecord(meta, &m_data, &m_key, &m_payload);
    auto m_key_size = meta.GetKeyLength();
    offset -= meta.GetTotalLength();
    (*new_node)->record_metadata[insert_idx].
        FinalizeForInsert(offset, m_key_size, meta.GetTotalLength());
    auto ptr = reinterpret_cast<char *>(*new_node) + offset;
    if (i == meta_to_update) {
      memcpy(ptr, m_data, meta.GetKeyLength());
      memcpy(ptr + meta.GetPaddedKeyLength(), &new_child_ptr, sizeof(uint64_t));
    } else {
      memcpy(ptr, m_data, meta.GetTotalLength());
    }
    insert_idx += 1;
  }
  (*new_node)->header.sorted_count = insert_idx;
#ifdef PMEM
  pmwcas::NVRAM::Flush((*new_node)->header.size, *new_node);
#endif
}

ReturnCode BaseNode::CheckMerge(bztree::Stack *stack, const char *key,
                                uint32_t key_size, bool backoff) {
  uint32_t merge_threshold = stack->tree->parameters.merge_threshold;
  auto pmwcas_pool = stack->tree->GetPMWCASPool();
  auto epoch = pmwcas_pool->GetEpoch();
  if (!IsLeaf() && GetHeader()->size > merge_threshold) {
    // we're internal node, large enough, we are good
    return ReturnCode::Ok();
  } else {
    // we're leaf node, large enough
    auto old_status = GetHeader()->GetStatus(epoch);
    auto valid_size = LeafNode::GetUsedSpace(old_status) - old_status.GetDeletedSize();
    if (valid_size > merge_threshold) {
      return ReturnCode::Ok();
    }
  }

  // too small, trying to merge siblings
  // start by checking parent node
  auto parent_frame = stack->Pop();
  if (!parent_frame) {
    // we are root node, ok
    return ReturnCode::Ok();
  }

  InternalNode *parent = parent_frame->node;
  uint32_t sibling_index = 0;

  auto should_merge = [&](uint32_t meta_index) -> bool {
    if (meta_index < 0 || meta_index >= parent->GetHeader()->sorted_count) {
      return false;
    }
    auto sibling = parent->GetChildByMetaIndex(meta_index, epoch);
    if (sibling->IsLeaf()) {
      auto status = sibling->GetHeader()->GetStatus(epoch);
      auto valid_size = LeafNode::GetUsedSpace(status) - status.GetDeletedSize();
      return valid_size < merge_threshold;
    } else {
      return sibling->GetHeader()->size < merge_threshold;
    }
  };

  if (should_merge(parent_frame->meta_index - 1)) {
    sibling_index = parent_frame->meta_index - 1;
  } else if (should_merge(parent_frame->meta_index + 1)) {
    sibling_index = parent_frame->meta_index + 1;
  } else {
    // Both left sibling and right sibling are good, we stay unchanged
    LOG_IF(INFO, !backoff) << "return with backoff false, meta_index:" << parent_frame->meta_index;
    if (!backoff) {
    }
    return ReturnCode::Ok();
  }

  // we found a suitable sibling
  // do the real merge
  BaseNode *sibling = parent->GetChildByMetaIndex(sibling_index, epoch);

  // Phase 1: freeze both nodes, and their parent
  auto node_status = this->GetHeader()->GetStatus(epoch);
  auto sibling_status = sibling->GetHeader()->GetStatus(epoch);
  auto parent_status = parent->GetHeader()->GetStatus(epoch);

  if (backoff && (node_status.IsFrozen() ||
      sibling_status.IsFrozen() || parent_status.IsFrozen())) {
    return ReturnCode::NodeFrozen();
  }

  auto *pd = pmwcas_pool->AllocateDescriptor();
  pd->AddEntry(&(&this->GetHeader()->status)->word,
               node_status.word, node_status.Freeze().word);
  pd->AddEntry(&(&sibling->GetHeader()->status)->word,
               sibling_status.word, sibling_status.Freeze().word);
  pd->AddEntry(&(&parent->GetHeader()->status)->word,
               parent_status.word, parent_status.Freeze().word);
  if (!pd->MwCAS()) {
    return ReturnCode::PMWCASFailure();
  }

  // Phase 2: allocate parent and new node
  pd = pmwcas_pool->AllocateDescriptor();
  pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                         reinterpret_cast<uint64_t>(nullptr),
                         pmwcas::Descriptor::kRecycleNewOnFailure);
  pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                         reinterpret_cast<uint64_t>(nullptr),
                         pmwcas::Descriptor::kRecycleNewOnFailure);
  auto *new_parent = reinterpret_cast<InternalNode **>(pd->GetNewValuePtr(0));
  auto *new_node = reinterpret_cast<BaseNode **>(pd->GetNewValuePtr(1));

  // lambda wrapper for merge leaf nodes
  auto merge_leaf_nodes = [&](uint32_t left_index, LeafNode *left_node, LeafNode *right_node) {
    LeafNode::MergeNodes(left_node, right_node,
                         reinterpret_cast<LeafNode **>(new_node));
    parent->DeleteRecord(left_index,
                         reinterpret_cast<uint64_t>(*new_node),
                         new_parent);
  };
  // lambda wrapper for merge internal nodes
  auto merge_internal_nodes = [&](uint32_t left_node_index,
                                  InternalNode *left_node, InternalNode *right_node) {
    // get the key for right node
    RecordMetadata right_meta = parent->record_metadata[left_node_index + 1];
    char *new_key = nullptr;
    parent->GetRawRecord(right_meta, nullptr, &new_key, nullptr);
    assert(right_meta.GetKeyLength() != 0);
    InternalNode::MergeNodes(left_node, right_node, new_key, right_meta.GetKeyLength(),
                             reinterpret_cast<InternalNode **> (new_node));
    parent->DeleteRecord(left_node_index,
                         reinterpret_cast<uint64_t>(*new_node),
                         new_parent);
  };

  // Phase 3: merge and init nodes
  if (sibling_index < parent_frame->meta_index) {
    IsLeaf() ?
    merge_leaf_nodes(sibling_index,
                     reinterpret_cast<LeafNode *>(sibling),
                     reinterpret_cast<LeafNode *>(this)) :
    merge_internal_nodes(sibling_index,
                         reinterpret_cast<InternalNode *>(sibling),
                         reinterpret_cast<InternalNode *>(this));
  } else {
    IsLeaf() ?
    merge_leaf_nodes(parent_frame->meta_index,
                     reinterpret_cast<LeafNode *>(this),
                     reinterpret_cast<LeafNode *>(sibling)) :
    merge_internal_nodes(parent_frame->meta_index,
                         reinterpret_cast<InternalNode *>(this),
                         reinterpret_cast<InternalNode *>(sibling));
  }

  // Phase 4: install new nodes
  auto grandpa_frame = stack->Top();
  ReturnCode rc;
  if (!grandpa_frame) {
    rc = stack->tree->ChangeRoot(reinterpret_cast<uint64_t>(stack->GetRoot()),
                                 reinterpret_cast<uint64_t>(*new_parent), pd) ?
         ReturnCode::Ok() : ReturnCode::PMWCASFailure();
    return rc;
  } else {
    InternalNode *grandparent = grandpa_frame->node;
    rc = grandparent->Update(grandparent->GetMetadata(grandpa_frame->meta_index, epoch),
                             parent, *new_parent, pd, pmwcas_pool);
    if (!rc.IsOk()) {
      return rc;
    }

    uint32_t freeze_retry = 0;
    do {
      // if previous merge succeeded, we move on to check new_parent
      rc = (*new_parent)->CheckMerge(stack, key, key_size,
                                     freeze_retry < MAX_FREEZE_RETRY);
      if (rc.IsOk()) {
        return rc;
      }
      freeze_retry += 1;
      stack->Clear();
      BaseNode *landed_on = stack->tree->TraverseToNode(stack, key, key_size, *new_parent);
      if (landed_on != *new_parent) {
        // we landed on a leaf node
        // means the *new_parent has been swapped out
        // either splitted or merged
        assert(landed_on->IsLeaf());
        return ReturnCode::Ok();
      }
    } while (rc.IsNodeFrozen() || rc.IsPMWCASFailure());
    assert(false);
  }
  return rc;
}

ReturnCode InternalNode::Update(RecordMetadata meta,
                                InternalNode *old_child,
                                InternalNode *new_child,
                                pmwcas::Descriptor *pd,
                                pmwcas::DescriptorPool *pmwcas_pool) {
  auto status = header.GetStatus(pmwcas_pool->GetEpoch());
  if (status.IsFrozen()) {
    return ReturnCode::NodeFrozen();
  }

  // Conduct a 2-word PMwCAS to swap in the new child pointer while ensuring the
  // node isn't frozen by a concurrent thread
  pd->AddEntry(&(&header.status)->word, status.word, status.word);
  pd->AddEntry(GetPayloadPtr(meta),
               reinterpret_cast<uint64_t>(old_child),
               reinterpret_cast<uint64_t>(new_child));
  if (pd->MwCAS()) {
    return ReturnCode::Ok();
  } else {
    return ReturnCode::PMWCASFailure();
  }
}

uint32_t InternalNode::GetChildIndex(const char *key,
                                     uint16_t key_size,
                                     bool get_le) {
  // Keys in internal nodes are always sorted, visible
  int32_t left = 0, right = header.sorted_count - 1, mid = 0;
  while (true) {
    mid = (left + right) / 2;
    auto meta = record_metadata[mid];
    char *record_key = nullptr;
    GetRawRecord(meta, nullptr, &record_key, nullptr, nullptr);
    auto cmp = KeyCompare(key, key_size, record_key, meta.GetKeyLength());
    if (cmp == 0) {
      // Key exists
      if (get_le) {
        return static_cast<uint32_t>(mid - 1);
      } else {
        return static_cast<uint32_t>(mid);
      }
    }
    if (left > right) {
      if (cmp <= 0 && get_le) {
        return static_cast<uint32_t>(mid - 1);
      } else {
        return static_cast<uint32_t> (mid);
      }
    } else {
      if (cmp > 0) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
  }
}

bool InternalNode::MergeNodes(InternalNode *left_node,
                              InternalNode *right_node,
                              const char *key, uint32_t key_size,
                              InternalNode **new_node) {
  uint32_t padded_keysize = RecordMetadata::PadKeyLength(key_size);
  uint32_t offset = left_node->header.size + right_node->header.size +
      padded_keysize - sizeof(InternalNode);
  InternalNode::New(new_node, offset);
  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  uint32_t cur_record = 0;

  InternalNode *node = *new_node;

  for (uint32_t i = 0; i < left_node->header.sorted_count; i += 1) {
    RecordMetadata meta = left_node->record_metadata[i];
    uint64_t payload;
    char *data;
    char *record_key;
    left_node->GetRawRecord(meta, &data, &record_key, &payload);
    assert(meta.GetTotalLength() >= sizeof(uint64_t));
    uint64_t total_len = meta.GetTotalLength();
    offset -= total_len;
    memcpy(reinterpret_cast<char *>(node) + offset,
           data, total_len);

    node->record_metadata[cur_record].FinalizeForInsert(offset, meta.GetKeyLength(), total_len);
    cur_record += 1;
  }
  for (uint32_t i = 0; i < right_node->header.sorted_count; i += 1) {
    RecordMetadata meta = right_node->record_metadata[i];
    uint64_t payload;
    char *cur_key;
    right_node->GetRawRecord(meta, nullptr, &cur_key, &payload);
    if (i == 0) {
      offset -= (padded_keysize + sizeof(uint64_t));
      memcpy(reinterpret_cast<char *>(node) + offset,
             key, key_size);
      memcpy(reinterpret_cast<char *>(node) + offset + padded_keysize,
             &payload, sizeof(uint64_t));
      node->record_metadata[cur_record].
          FinalizeForInsert(offset, key_size, padded_keysize + sizeof(uint64_t));
    } else {
      assert(meta.GetTotalLength() >= sizeof(uint64_t));
      uint64_t total_len = meta.GetTotalLength();
      offset -= total_len;
      memcpy(reinterpret_cast<char *>(node) + offset,
             cur_key, total_len);
      node->record_metadata[cur_record].FinalizeForInsert(offset, meta.GetKeyLength(), total_len);
    }
    cur_record += 1;
  }
  node->header.sorted_count = cur_record;
#ifdef PMDK
  Allocator::Get()->PersistPtr(node, node->header.size);
#endif
  return true;
}

bool LeafNode::MergeNodes(LeafNode *left_node, LeafNode *right_node, LeafNode **new_node) {
  LeafNode::New(new_node, left_node->header.size);
  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  auto copy_metadata = [](std::vector<RecordMetadata> *meta_vec, LeafNode *node) -> uint32_t {
    uint32_t count = node->header.status.GetRecordCount();
    uint32_t valid_count = 0;
    for (uint32_t i = 0; i < count; i++) {
      RecordMetadata meta = node->record_metadata[i];
      if (meta.IsVisible()) {
        meta_vec->emplace_back(meta);
        valid_count += 1;
      }
    }
    return valid_count;
  };
  uint32_t left_count = copy_metadata(&meta_vec, left_node);
  uint32_t right_count = copy_metadata(&meta_vec, right_node);

  auto key_cmp = [](LeafNode *node) {
    return [node](RecordMetadata &m1, RecordMetadata &m2) {
      return KeyCompare(node->GetKey(m1), m1.GetKeyLength(),
                        node->GetKey(m2), m2.GetKeyLength()) < 0;
    };
  };
  // todo(hao): make sure these lambdas are zero-overhead
  // note: left half is always smaller than the right half
  std::sort(meta_vec.begin(), meta_vec.begin() + left_count, key_cmp(left_node));
  std::sort(meta_vec.begin() + left_count,
            meta_vec.begin() + left_count + right_count, key_cmp(right_node));

  LeafNode *node = *new_node;
  uint32_t offset = node->header.size;
  uint32_t cur_record = 0;
  for (auto meta_iter = meta_vec.begin(); meta_iter < meta_vec.end(); meta_iter++) {
    uint64_t payload;
    char *key;
    if (meta_iter < meta_vec.begin() + left_count) {
      left_node->GetRawRecord(*meta_iter, &key, &payload);
    } else {
      right_node->GetRawRecord(*meta_iter, &key, &payload);
    }

    assert(meta_iter->GetTotalLength() >= sizeof(uint64_t));
    uint64_t total_len = meta_iter->GetTotalLength();
    offset -= total_len;
    char *ptr = reinterpret_cast<char *>(node) + offset;
    memcpy(ptr, key, total_len);

    node->record_metadata[cur_record].
        FinalizeForInsert(offset, meta_iter->GetKeyLength(), total_len);
    cur_record += 1;
  }
  node->header.status.SetBlockSize(node->header.size - offset);
  node->header.status.SetRecordCount(cur_record);
  node->header.sorted_count = cur_record;
#ifdef PMDK
  Allocator::Get()->PersistPtr(node, node->header.size);
#endif
  return true;
}

bool LeafNode::PrepareForSplit(Stack &stack,
                               uint32_t split_threshold,
                               pmwcas::Descriptor *pd,
                               pmwcas::DescriptorPool *pmwcas_pool,
                               LeafNode **left, LeafNode **right,
                               InternalNode **new_parent,
                               bool backoff) {
  ALWAYS_ASSERT(header.GetStatus(pmwcas_pool->GetEpoch()).GetRecordCount() > 2);

  // Prepare new nodes: a parent node, a left leaf and a right leaf
  LeafNode::New(left, this->header.size);
  LeafNode::New(right, this->header.size);

  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();
  uint32_t total_size = SortMetadataByKey(meta_vec, true, pmwcas_pool->GetEpoch());

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

  assert(nleft > 0);

  // TODO(tzwang): also put the new insert here to save some cycles
  auto left_end_it = meta_vec.begin() + nleft;
#ifdef PMDK
  (Allocator::Get()->GetDirect(*left))->CopyFrom(this, meta_vec.begin(),
                                                 left_end_it, pmwcas_pool->GetEpoch());
  (Allocator::Get()->GetDirect(*right))->CopyFrom(this, left_end_it,
                                                  meta_vec.end(), pmwcas_pool->GetEpoch());
#else
  (*left)->CopyFrom(this, meta_vec.begin(), left_end_it, pmwcas_pool->GetEpoch());
  (*right)->CopyFrom(this, left_end_it, meta_vec.end(), pmwcas_pool->GetEpoch());
#endif

  // Separator exists in the new left leaf node, i.e., when traversing the tree,
  // we go left if <=, and go right if >.
  RecordMetadata separator_meta = meta_vec[nleft - 1];

  // The node is already frozen (by us), so we must be able to get a valid key
  char *key = GetKey(separator_meta);
  assert(key);

  InternalNode *parent = stack.Top() ?
                         stack.Top()->node : nullptr;
  if (parent == nullptr) {
    // Good boy!
    InternalNode::New(key, separator_meta.GetKeyLength(),
                      reinterpret_cast<uint64_t>(*left),
                      reinterpret_cast<uint64_t>(*right),
                      new_parent);
    return true;
  }

  bool frozen_by_me = false;
  while (!parent->IsFrozen(pmwcas_pool->GetEpoch())) {
    frozen_by_me = parent->Freeze(pmwcas_pool);
  }

  if (!frozen_by_me && backoff) {
    return false;
  } else {
    // Has a parent node. PrepareForSplit will see if we need to split this
    // parent node as well, and if so, return a new (possibly upper-level) parent
    // node that needs to be installed to its parent
    return parent->PrepareForSplit(stack, split_threshold, key,
                                   separator_meta.GetKeyLength(),
                                   reinterpret_cast<uint64_t>(*left),
                                   reinterpret_cast<uint64_t>(*right),
                                   new_parent,
                                   pd, pmwcas_pool,
                                   backoff);
  }
}

BaseNode *BzTree::TraverseToNode(bztree::Stack *stack,
                                 const char *key, uint16_t key_size,
                                 bztree::BaseNode *stop_at,
                                 bool le_child) {
  BaseNode *node = GetRootNodeSafe();
  if (stack) {
    stack->SetRoot(node);
  }
  InternalNode *parent = nullptr;
  uint32_t meta_index = 0;
  while (node != stop_at && !node->IsLeaf()) {
    assert(!node->IsLeaf());
    parent = reinterpret_cast<InternalNode *>(node);
    meta_index = parent->GetChildIndex(key, key_size, le_child);
    node = parent->GetChildByMetaIndex(meta_index, GetPMWCASPool()->GetEpoch());
    assert(node);
    if (stack != nullptr) {
      stack->Push(parent, meta_index);
    }
  }
  return node;
}

LeafNode *BzTree::TraverseToLeaf(Stack *stack, const char *key,
                                 uint16_t key_size,
                                 bool le_child) {
  BaseNode *node = GetRootNodeSafe();
  if (stack) {
    stack->SetRoot(node);
  }
  InternalNode *parent = nullptr;
  uint32_t meta_index = 0;
  assert(node);
  while (!node->IsLeaf()) {
    parent = reinterpret_cast<InternalNode *>(node);
    meta_index = parent->GetChildIndex(key, key_size, le_child);
    node = parent->GetChildByMetaIndex(meta_index, GetPMWCASPool()->GetEpoch());
    assert(node);
    if (stack != nullptr) {
      stack->Push(parent, meta_index);
    }
  }
  return reinterpret_cast<LeafNode *>(node);
}

ReturnCode BzTree::Insert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  uint64_t freeze_retry = 0;

  while (true) {
    stack.Clear();
    pmwcas::EpochGuard guard(GetPMWCASPool()->GetEpoch());
    LeafNode *node = TraverseToLeaf(&stack, key, key_size);

    // Try to insert to the leaf node
    auto rc = node->Insert(key, key_size, payload, GetPMWCASPool(), parameters.split_threshold);
    if (rc.IsOk() || rc.IsKeyExists()) {
      return rc;
    }

    assert(rc.IsNotEnoughSpace() || rc.IsNodeFrozen());
    if (rc.IsNodeFrozen()) {
      if (++freeze_retry <= MAX_FREEZE_RETRY) {
        continue;
      }
    } else {
      bool frozen_by_me = false;
      while (!node->IsFrozen(GetPMWCASPool()->GetEpoch())) {
        frozen_by_me = node->Freeze(GetPMWCASPool());
      }
      if (!frozen_by_me && ++freeze_retry <= MAX_FREEZE_RETRY) {
        continue;
      }
    }

    bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);

    // Should split and we have three cases to handle:
    // 1. Root node is a leaf node - install [parent] as the new root
    // 2. We have a parent but no grandparent - install [parent] as the new
    //    root
    // 3. We have a grandparent - update the child pointer in the grandparent
    //    to point to the new [parent] (might further cause splits up the tree)

    auto *pd = GetPMWCASPool()->AllocateDescriptor();
    // TODO(hao): should implement a cascading memory recycle callback
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleNewOnFailure);
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleNewOnFailure);
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleNewOnFailure);
    uint64_t *ptr_r = pd->GetNewValuePtr(0);
    uint64_t *ptr_l = pd->GetNewValuePtr(1);
    uint64_t *ptr_parent = pd->GetNewValuePtr(2);

    // Note that when we split internal nodes (if needed), stack will get
    // Pop()'ed recursively, leaving the grantparent as the top (if any) here.
    // So we save the root node here in case we need to change root later.

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
    bool should_proceed = node->PrepareForSplit(stack,
                                                parameters.split_threshold,
                                                pd, GetPMWCASPool(),
                                                reinterpret_cast<LeafNode **>(ptr_l),
                                                reinterpret_cast<LeafNode **>(ptr_r),
                                                reinterpret_cast<InternalNode **>(ptr_parent),
                                                backoff);
    if (!should_proceed) {
      // TODO(tzwang): free memory allocated in ptr_l, ptr_r, and ptr_parent
      continue;
    }

    assert(*ptr_parent);

    auto *top = stack.Pop();
    InternalNode *old_parent = nullptr;
    if (top) {
      old_parent = top->node;
    }

    top = stack.Pop();
    InternalNode *grand_parent = nullptr;
    if (top) {
      grand_parent = top->node;
    }

    if (grand_parent) {
      assert(old_parent);
      // There is a grand parent. We need to swap out the pointer to the old
      // parent and install the pointer to the new parent.
#ifdef PMDK
      auto result = grand_parent->Update(
          top->node->GetMetadata(top->meta_index, GetPMWCASPool()->GetEpoch()),
          Allocator::Get()->GetOffset(old_parent),
          reinterpret_cast<InternalNode *>(*ptr_parent), pd, GetPMWCASPool());
#else
      auto result = grand_parent->Update(
          top->node->GetMetadata(top->meta_index, GetPMWCASPool()->GetEpoch()),
          old_parent, reinterpret_cast<InternalNode *>(*ptr_parent), pd, GetPMWCASPool());
#endif
    } else {
      // No grand parent or already popped out by during split propagation
      // In case of PMDK, ptr_parent is already in PMDK offset format (done by
      // InternalNode::New).
#ifdef PMDK
      ChangeRoot(reinterpret_cast<uint64_t>(Allocator::Get()->GetOffset(stack.GetRoot())),
                 *ptr_parent, pd);
#else
      ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), *ptr_parent, pd);
#endif
    }
  }
}

bool BzTree::ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr,
                        pmwcas::Descriptor *pd) {
  // Memory policy here is "Never" because the memory was allocated in
  // PrepareForInsert/BzTree::Insert which uses a descriptor that specifies
  // policy RecycleOnRecovery
  pd->AddEntry(reinterpret_cast<uint64_t *>(&root), expected_root_addr, new_root_addr,
               pmwcas::Descriptor::kRecycleNever);
  return pd->MwCAS();
}

ReturnCode BzTree::Read(const char *key, uint16_t key_size, uint64_t *payload) {
  thread_local Stack stack;
  stack.tree = this;
  stack.Clear();
  pmwcas::EpochGuard guard(GetPMWCASPool()->GetEpoch());

  LeafNode *node = TraverseToLeaf(&stack, key, key_size);
  if (node == nullptr) {
    return ReturnCode::NotFound();
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload, GetPMWCASPool());
  if (rc.IsOk()) {
    *payload = tmp_payload;
  }
  return rc;
}

ReturnCode BzTree::Update(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  ReturnCode rc;
  pmwcas::EpochGuard guard(GetPMWCASPool()->GetEpoch());
  do {
    stack.Clear();
    LeafNode *node = TraverseToLeaf(&stack, key, key_size, GetPMWCASPool());
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Update(key, key_size, payload, GetPMWCASPool());
  } while (rc.IsPMWCASFailure());
  return rc;
}

ReturnCode BzTree::Upsert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  stack.Clear();
  pmwcas::EpochGuard guard(GetPMWCASPool()->GetEpoch());

  LeafNode *node = TraverseToLeaf(&stack, key, key_size, GetPMWCASPool());
  // FIXME(tzwang): be more clever here to get the node this record would be
  // landing in?
  if (node == nullptr) {
    return Insert(key, key_size, payload);
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload, GetPMWCASPool());
  if (rc.IsNotFound()) {
    return Insert(key, key_size, payload);
  } else if (rc.IsOk()) {
    if (tmp_payload == payload) {
      return ReturnCode::Ok();
    }
    return Update(key, key_size, payload);
  } else {
    return rc;
  }
}

ReturnCode BzTree::Delete(const char *key, uint16_t key_size) {
  thread_local Stack stack;
  stack.tree = this;
  ReturnCode rc;
  auto *epoch = GetPMWCASPool()->GetEpoch();
  pmwcas::EpochGuard guard(epoch);
  LeafNode *node;
  do {
    stack.Clear();
    node = TraverseToLeaf(&stack, key, key_size, GetPMWCASPool());
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Delete(key, key_size, GetPMWCASPool());
  } while (rc.IsNodeFrozen());

  if (!rc.IsOk() || ENABLE_MERGE == 0) {
    // delete failed
    return rc;
  }

  // finished record delete, now check if we can merge siblings
  uint32_t freeze_retry = 0;
  do {
    rc = node->CheckMerge(&stack, key, key_size, freeze_retry < MAX_FREEZE_RETRY);
    if (rc.IsOk()) {
      return rc;
    }
    stack.Clear();
    node = TraverseToLeaf(&stack, key, key_size, GetPMWCASPool());
    if (rc.IsNodeFrozen()) {
      freeze_retry += 1;
    }
  } while (rc.IsNodeFrozen() || rc.IsPMWCASFailure());
  assert(false);
}

void BzTree::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << "Dumping tree with root node: " << root << std::endl;
  // Traverse each level and dump each node
  auto real_root = GetRootNodeSafe();
  if (real_root->IsLeaf()) {
    (reinterpret_cast<LeafNode *>(real_root)->Dump(GetPMWCASPool()->GetEpoch()));
  } else {
    (reinterpret_cast<InternalNode *>(real_root))->Dump(
        GetPMWCASPool()->GetEpoch(), true /* inlcude children */);
  }
}

}  // namespace bztree
