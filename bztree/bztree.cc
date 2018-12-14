#include <iostream>
#include <cstring>

#include "bztree.h"

namespace bztree {

InternalNode *InternalNode::New(uint32_t data_size, uint32_t sorted_count) {
  // FIXME(tzwang): use a better allocator
  uint32_t alloc_size = sizeof(InternalNode) + data_size;
  InternalNode *node = (InternalNode *) malloc(alloc_size);
  memset(node, 0, alloc_size);
  new(node) InternalNode(data_size, sorted_count);
  return node;
}

LeafNode *LeafNode::New() {
  // FIXME(tzwang): use a better allocator
  LeafNode *node = (LeafNode *) malloc(kNodeSize);
  memset(node, 0, kNodeSize);
  new(node) LeafNode;
  return node;
}

void LeafNode::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << " Dumping node: 0x" << this << std::endl;
  std::cout << " Header:\n"
            << " - size: " << header.size << std::endl
            << " - status: 0x" << std::hex << header.status.word << std::endl
            << "   (control = 0x" << (header.status.word & NodeHeader::StatusWord::kControlMask)
            << std::dec
            << ", frozen = " << header.status.IsFrozen()
            << ", block size = " << header.status.GetBlockSize()
            << ", delete size = " << header.status.GetDeleteSize()
            << ", record cout = " << header.status.GetRecordCount() << ")\n"
            << " - sorted_count: " << header.sorted_count
            << std::endl;

  std::cout << " Record Metadata Array:" << std::endl;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    BaseNode::RecordMetadata meta = record_metadata[i];
    std::cout << " - record " << i << ": meta = 0x" << std::hex << meta.meta << std::endl;
    std::cout << std::hex;
    std::cout << "   (control = 0x" << (meta.meta & BaseNode::RecordMetadata::kControlMask)
              << std::dec
              << ", visible = " << meta.IsVisible()
              << ", offset = " << meta.GetOffset()
              << ", key length = " << meta.GetKeyLength()
              << ", total length = " << meta.GetTotalLength()
              << std::endl;
  }

  std::cout << " Key-Payload Pairs:" << std::endl;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    BaseNode::RecordMetadata meta = record_metadata[i];
    uint64_t payload = 0;
    char *key = GetRecord(meta, payload);
    std::string keystr(key, key + meta.GetKeyLength());
    std::cout << " - record " << i << ": key = " << keystr
              << ", payload = " << payload << std::endl;
  }

  std::cout << "-----------------------------" << std::endl;
}

bool LeafNode::Insert(uint32_t epoch, char *key, uint32_t key_size, uint64_t payload,
                      pmwcas::DescriptorPool *pmwcas_pool) {
  retry:
  NodeHeader::StatusWord expected_status = header.status;

  // If frozon then retry
  if (expected_status.IsFrozen()) {
    return false;
  }

  auto uniqueness = CheckUnique(key, key_size);
  if (uniqueness == Duplicate) {
    return false;
  }

  // Now try to reserve space in the free space region using a PMwCAS. Two steps:
  // Step 1. Incrementing the record count and block size fields in [status] 
  // Step 2. Flip the record metadata entry's high order bit and fill in global
  // epoch
  NodeHeader::StatusWord desired_status = expected_status;

  // Block size includes both key and payload sizes
  uint64_t total_size = key_size + sizeof(payload);
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
    return false;
  }

  // Reserved space! Now copy data
  uint64_t offset = kNodeSize - desired_status.GetBlockSize();
  char *ptr = &((char *) this)[offset];
  memcpy(ptr, key, key_size);
  memcpy(ptr + key_size, &payload, sizeof(payload));

  // Flush the word
  pmwcas::NVRAM::Flush(key_size + sizeof(payload), ptr);

  if (uniqueness == ReCheck) {
    uniqueness = RecheckUnique(key, key_size, expected_status.GetRecordCount());
    if (uniqueness == Duplicate) {
      memset(ptr, 0, key_size);
      memset(ptr + key_size, 0, sizeof(payload));
      offset = 0;
    }
  }

  // Re-check if the node is frozen
  NodeHeader::StatusWord s = header.status;
  if (s.IsFrozen()) {
    return false;
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
    return pd->MwCAS();
  }
}

LeafNode::Uniqueness LeafNode::CheckUnique(const char *key, uint32_t key_size) {
//  Binary search on sorted field
  uint32_t first = 0;
  uint32_t last = header.sorted_count - 1;
  uint32_t middle;
  while (header.sorted_count != 0 && first <= last) {
    middle = (first + last) / 2;
    uint64_t payload = 0;
    auto &current = record_metadata[middle];
    auto current_key = GetRecord(current, payload);
    auto cmp_result = memcmp(key, current_key, current.GetKeyLength());
    if (cmp_result < 0) {
      first = middle + 1;
    } else if (cmp_result == 0 && key_size == current.GetKeyLength()) {
      return Duplicate;
    } else {
      last = middle - 1;
    }
  }

//  Linear search on unsorted field
  for (uint32_t i = header.sorted_count; i < header.status.GetRecordCount(); i++) {
    auto &current = record_metadata[i];

//    Encountered an in-progress insert, recheck later
    if (current.IsVisible() == 0) {
      auto offset = current.GetOffset();
//      FIXME(hao): global epoch may not be zero
      if ((offset & uint64_t{0xFFFFFFF}) == 0) {
        return ReCheck;
      }
    }
    uint64_t payload = 0;
    auto current_key = GetRecord(current, payload);
    if (key_size == current.GetKeyLength() &&
        std::strncmp(key, current_key, current.GetKeyLength()) == 0) {
      return Duplicate;
    }
  }

  return IsUnique;
}

LeafNode::Uniqueness LeafNode::RecheckUnique(const char *key, uint32_t key_size, uint64_t end_pos) {
  for (uint32_t i = header.sorted_count; i < end_pos; i++) {
    retry:
    auto &current = record_metadata[i];

//    Encountered an operation serialized behind the insert
//    Must wait for the record to be visible
    if (current.IsVisible() == 0) {
      auto offset = current.GetOffset();
      if ((offset & uint64_t{0xFFFFFFF}) == 0) {
        goto retry;
      }
    }
    uint64_t payload = 0;
    auto current_key = GetRecord(current, payload);
    if (key_size == current.GetKeyLength() &&
        std::strncmp(key, current_key, current.GetKeyLength()) == 0) {
      return Duplicate;
    }
  }
  return IsUnique;
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
  thread_local std::vector<RecordMetadata> meta_vec;
  meta_vec.clear();

  // Freeze the node to prevent new modifications first
  if (!Freeze(pmwcas_pool)) {
    return nullptr;
  }

  uint32_t total_size = 0;
  for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
    // TODO(tzwang): handle deletes
    if (record_metadata[i].IsVisible()) {
      auto meta = record_metadata[i];
      meta_vec.emplace_back(meta);
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

  std::sort(meta_vec.begin(), meta_vec.end(), key_cmp);

  // Allocate and populate a new node
  LeafNode *new_leaf = LeafNode::New();

  // Set proper header fields
  new_leaf->header.size = total_size;
  new_leaf->header.status.word = (total_size << 20) | (meta_vec.size() << 4);
  new_leaf->header.sorted_count = meta_vec.size();

  // Now meta_vec is in sorted order, insert records one by one
  uint64_t offset = kNodeSize;
  for (uint32_t i = 0; i < meta_vec.size(); ++i) {
    auto &meta = meta_vec[i];
    uint64_t payload = 0;
    char *key = GetRecord(meta, payload);

    uint64_t total_len = meta.GetTotalLength();
    offset -= total_len;
    char *ptr = &((char *) new_leaf)[offset];
    memcpy(ptr, key, total_len);

    BaseNode::RecordMetadata new_meta = meta;
    new_meta.FinalizeForInsert(offset, meta.GetKeyLength(), total_len);
    new_leaf->record_metadata[i] = new_meta;
  }

  pmwcas::NVRAM::Flush(kNodeSize, new_leaf);

  return new_leaf;
}

BaseNode *InternalNode::GetChild(char *key, uint64_t key_size) {
  // Keys in internal nodes are always sorted
  int32_t left = 0, right = header.status.GetRecordCount() - 1;
  while (left <= right) {
    uint32_t mid = (left + right) / 2;
    auto meta = record_metadata[mid];
    uint64_t meta_key_size = meta.GetKeyLength();
    uint64_t meta_payload = 0;
    char *meta_key = GetRecord(meta, meta_payload);
    int cmp = memcmp(key, meta_key, std::min<uint64_t>(meta_key_size, key_size));
    if (cmp == 0) {
      if (meta_key_size == key_size) {
        // Key exists
        left = mid;
        break;
      }
    }
    if (cmp > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  LOG_IF(FATAL, left < 0);

  auto meta = record_metadata[left];
  uint64_t meta_payload = 0;
  GetRecord(meta, meta_payload);
  return (BaseNode *) meta_payload;
}

LeafNode *BzTree::TraverseToLeaf(Stack &stack, char *key, uint64_t key_size) {
  BaseNode *node = root;
  while (!node->IsLeaf()) {
    stack.Push((InternalNode *) node);
    node = ((InternalNode *) node)->GetChild(key, key_size);
  }
  return (LeafNode *) node;
}

bool BzTree::Insert(char *key, uint64_t key_size) {
}

}  // namespace bztree
