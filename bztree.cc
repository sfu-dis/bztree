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

BzTree::BzTree(const bztree::ParameterSet &param, bztree::nv_ptr<pmwcas::DescriptorPool> pool, uint64_t pmdk_addr)
    : parameters(param),
      root(),
      pmdk_addr(pmdk_addr),
      index_epoch(0),
      pmwcas_pool(pool) {
  global_epoch = index_epoch;
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
  auto *pd = pool->AllocateDescriptor();
  auto index = pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(&root),
                                      reinterpret_cast<uint64_t>(nullptr),
                                      pmwcas::Descriptor::kRecycleOnRecovery);
  auto root_ptr = pd->GetNewValuePtr(index);
  LeafNode::New(reinterpret_cast<LeafNode **>(root_ptr), param.leaf_node_size);
  pd->MwCAS();
}

#ifdef PMEM
void BzTree::Recovery() {
  index_epoch += 1;
  // avoid multiple increment if there are multiple bztrees
  if (global_epoch != index_epoch) {
    global_epoch = index_epoch;
  }
  pmwcas_pool->Recovery(false);
  pmwcas::NVRAM::Flush(sizeof(bztree::BzTree), this);
}
#endif

BzTree *BzTree::New(const ParameterSet &param, nv_ptr<pmwcas::DescriptorPool> pool) {
  BzTree *tree;
  pmwcas::Allocator::Get()->Allocate(reinterpret_cast<void **>(&tree), sizeof(BzTree));
  new(tree) BzTree(param, pool);
  return tree;
}

nv_ptr<BaseNode> BzTree::GetRootNodeSafe() {
  auto root_node = reinterpret_cast<pmwcas::MwcTargetField<uint64_t> *>(
      &root)->GetValueProtected();
  return nv_ptr<BaseNode>(root_node);
}

BaseNode *BzTree::TraverseToNode(bztree::Stack *stack,
                                 const char *key, uint16_t key_size,
                                 bztree::BaseNode *stop_at,
                                 bool le_child) {
  nv_ptr<BaseNode> nv_node = GetRootNodeSafe();
  if (stack) {
    stack->SetRoot(nv_node);
  }
  auto node = nv_node.get_direct();
  InternalNode *parent = nullptr;
  uint32_t meta_index = 0;
  while (node != stop_at && !node->IsLeaf()) {
    assert(!node->IsLeaf());
    parent = reinterpret_cast<InternalNode *>(node);
    meta_index = parent->GetChildIndex(key, key_size, le_child);
    node = parent->GetChildByMetaIndex(meta_index, pmwcas_pool->GetEpoch());
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
  static const uint32_t kCacheLineSize = 64;
  nv_ptr<BaseNode> nv_node = GetRootNodeSafe();
  auto *node = nv_node.get_direct();
  __builtin_prefetch((const void *) (node), 0, 3);

  if (stack) {
    stack->SetRoot(nv_node);
  }
  InternalNode *parent = nullptr;
  uint32_t meta_index = 0;
  assert(node);
  while (!node->IsLeaf()) {
    parent = reinterpret_cast<InternalNode *>(node);
    meta_index = parent->GetChildIndex(key, key_size, le_child);
    node = parent->GetChildByMetaIndex(meta_index, pmwcas_pool->GetEpoch());
    __builtin_prefetch((const void *) (node), 0, 3);
    assert(node);
    if (stack != nullptr) {
      stack->Push(parent, meta_index);
    }
  }

  for (uint32_t i = 0; i < parameters.leaf_node_size / kCacheLineSize; ++i) {
    __builtin_prefetch((const void *) ((char *) node + i * kCacheLineSize), 0, 3);
  }
  return reinterpret_cast<LeafNode *>(node);
}

ReturnCode BzTree::Insert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  uint64_t freeze_retry = 0;

  while (true) {
    stack.Clear();
    pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
    LeafNode *node = TraverseToLeaf(&stack, key, key_size);

    // Try to insert to the leaf node
    auto rc = node->Insert(key, key_size, payload, pmwcas_pool, parameters.split_threshold);
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
      while (!node->IsFrozen()) {
        frozen_by_me = node->Freeze(pmwcas_pool);
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

    auto *pd = pmwcas_pool->AllocateDescriptor();
    // TODO(hao): should implement a cascading memory recycle callback
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleOnRecovery);
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleOnRecovery);
    pd->ReserveAndAddEntry(reinterpret_cast<uint64_t *>(pmwcas::Descriptor::kAllocNullAddress),
                           reinterpret_cast<uint64_t>(nullptr),
                           pmwcas::Descriptor::kRecycleOnRecovery);
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
                                                pd, pmwcas_pool,
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
          top->node->GetMetadata(top->meta_index),
          Allocator::Get()->GetOffset(old_parent),
          reinterpret_cast<InternalNode *>(*ptr_parent), pd);
#else
      auto result = grand_parent->Update(
          top->node->GetMetadata(top->meta_index),
          old_parent, reinterpret_cast<InternalNode *>(*ptr_parent), pd);
#endif
    } else {
      // No grand parent or already popped out by during split propagation
      // In case of PMDK, ptr_parent is already in PMDK offset format (done by
      // InternalNode::New).
      ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot().get_offset()), *ptr_parent, pd);
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
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());

  LeafNode *node = TraverseToLeaf(&stack, key, key_size);
  if (node == nullptr) {
    return ReturnCode::NotFound();
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload, pmwcas_pool);
  if (rc.IsOk()) {
    *payload = tmp_payload;
  }
  return rc;
}

ReturnCode BzTree::Update(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  ReturnCode rc;
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());
  do {
    stack.Clear();
    LeafNode *node = TraverseToLeaf(&stack, key, key_size);
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Update(key, key_size, payload, pmwcas_pool);
  } while (rc.IsPMWCASFailure());
  return rc;
}

ReturnCode BzTree::Upsert(const char *key, uint16_t key_size, uint64_t payload) {
  thread_local Stack stack;
  stack.tree = this;
  stack.Clear();
  pmwcas::EpochGuard guard(pmwcas_pool->GetEpoch());

  LeafNode *node = TraverseToLeaf(&stack, key, key_size);
  // FIXME(tzwang): be more clever here to get the node this record would be
  // landing in?
  if (node == nullptr) {
    return Insert(key, key_size, payload);
  }
  uint64_t tmp_payload;
  auto rc = node->Read(key, key_size, &tmp_payload, pmwcas_pool);
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
  auto *epoch = pmwcas_pool->GetEpoch();
  pmwcas::EpochGuard guard(epoch);
  LeafNode *node;
  do {
    stack.Clear();
    node = TraverseToLeaf(&stack, key, key_size);
    if (node == nullptr) {
      return ReturnCode::NotFound();
    }
    rc = node->Delete(key, key_size, pmwcas_pool);
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
    node = TraverseToLeaf(&stack, key, key_size);
    if (rc.IsNodeFrozen()) {
      freeze_retry += 1;
    }
  } while (rc.IsNodeFrozen() || rc.IsPMWCASFailure());
  ALWAYS_ASSERT(false);
  return rc;  // Just to silence the compiler
}

void BzTree::Dump() {
  std::cout << "-----------------------------" << std::endl;
  std::cout << "Dumping tree with root node: " << root.get_direct() << std::endl;
  // Traverse each level and dump each node
  auto real_root = GetRootNodeSafe();
  if (real_root->IsLeaf()) {
    (reinterpret_cast<LeafNode *>(real_root.get_direct())->Dump(pmwcas_pool->GetEpoch()));
  } else {
    (reinterpret_cast<InternalNode *>(real_root.get_direct()))->Dump(
        pmwcas_pool->GetEpoch(), true /* inlcude children */);
  }
}

}  // namespace bztree
