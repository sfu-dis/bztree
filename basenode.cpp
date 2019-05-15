// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include "basenode.h"
#include "bztree.h"

namespace bztree {

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

ReturnCode BaseNode::CheckMerge(Stack *stack, const char *key,
                                uint32_t key_size, bool backoff) {
  uint32_t merge_threshold = stack->tree->parameters.merge_threshold;
  auto pmwcas_pool = stack->tree->pmwcas_pool;
  auto epoch = pmwcas_pool->GetEpoch();
  if (!IsLeaf() && GetHeader()->size > merge_threshold) {
    // we're internal node, large enough, we are good
    return ReturnCode::Ok();
  } else {
    // we're leaf node, large enough
    auto old_status = GetHeader()->GetStatus();
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
      auto status = sibling->GetHeader()->GetStatus();
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
    if (!backoff) {
    }
    return ReturnCode::Ok();
  }

  // we found a suitable sibling
  // do the real merge
  BaseNode *sibling = parent->GetChildByMetaIndex(sibling_index, epoch);

  // Phase 1: freeze both nodes, and their parent
  auto node_status = this->GetHeader()->GetStatus();
  auto sibling_status = sibling->GetHeader()->GetStatus();
  auto parent_status = parent->GetHeader()->GetStatus();

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
                         pmwcas::Descriptor::kRecycleOnRecovery);
  pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                         reinterpret_cast<uint64_t>(nullptr),
                         pmwcas::Descriptor::kRecycleOnRecovery);
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
    rc = stack->tree->ChangeRoot(reinterpret_cast<uint64_t>(nv_ptr<BaseNode>(stack->GetRoot()).get_offset()),
                                 reinterpret_cast<uint64_t>(*new_parent), pd) ?
         ReturnCode::Ok() : ReturnCode::PMWCASFailure();
    return rc;
  } else {
    InternalNode *grandparent = grandpa_frame->node;
    rc = grandparent->Update(grandparent->GetMetadata(grandpa_frame->meta_index),
                             parent, *new_parent, pd);
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
}
