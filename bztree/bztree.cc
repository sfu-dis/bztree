#include <iostream>

#include "bztree.h"

namespace bztree {

/*
InternalNode *InternalNode::New() {
  // FIXME(tzwang): use a better allocator
  InternalNode *node = (InternalNode *)malloc(InternalNode::kNodeSize);
  new (node) InternalNode;
  return node;
}
*/

LeafNode *LeafNode::New() {
  // FIXME(tzwang): use a better allocator
  LeafNode *node = (LeafNode *)malloc(kNodeSize);
  memset(node, 0, kNodeSize);
  new (node) LeafNode;
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

  std::cout << " Record Metadata Array:" <<std::endl;
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

  std::cout << " Key-Payload Pairs:" <<std::endl;
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
  char *ptr = &((char*)this + kNodeSize)[offset];
  memcpy(ptr, key, key_size);
  memcpy(ptr + key_size, &payload, sizeof(payload));

  // Flush the word
  pmwcas::NVRAM::Flush(key_size + sizeof(payload), ptr);

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

}  // namespace bztree
