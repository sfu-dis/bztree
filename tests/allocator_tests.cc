// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "../allocator.h"

const char *pool_name = "test_pool";
const char *layout_name = "linked_list";

struct list {
  uint64_t value;
  PMEMoid next;
};

POBJ_LAYOUT_BEGIN(linked_list);
POBJ_LAYOUT_TOID(linked_list, list);
POBJ_LAYOUT_END(linked_list);

void init_pool() {
  auto allocator = Allocator::New(pool_name, layout_name);

  PMEMoid root = pmemobj_root(allocator->GetPool(), sizeof(list));
  list *raw_root = (list *) pmemobj_direct(root);

  raw_root->value = 21;
  raw_root->next = TOID_NULL(list).oid;

  pmemobj_persist(allocator->GetPool(), raw_root, sizeof(list));
}

TEST(AllocatorTest, MetaTest) {
  init_pool();
  auto allocator = Allocator::New(pool_name, layout_name);

  PMEMoid root = pmemobj_root(allocator->GetPool(), sizeof(list));
  list *raw_root = (list *) pmemobj_direct(root);
  assert(raw_root->value == 21);

  auto prev = raw_root;
  for (uint16_t i = 0; i < 32; i++) {
    PMEMoid tmp_ptr = allocator->Alloc(sizeof(list));
    auto tmp_raw = (list *) pmemobj_direct(tmp_ptr);
    tmp_raw->value = i;
    tmp_raw->next = TOID_NULL(list).oid;

    pmemobj_persist(allocator->GetPool(), tmp_raw, sizeof(list));

    prev->next = tmp_ptr;
    pmemobj_persist(allocator->GetPool(), &prev->next, sizeof(PMEMoid));

    prev = tmp_raw;
  }

  allocator.reset();
  allocator = Allocator::New(pool_name, layout_name);
  root = pmemobj_root(allocator->GetPool(), sizeof(list));
  raw_root = (list *) pmemobj_direct(root);
  assert(raw_root->value == 21);

  auto next = raw_root->next;
  for (uint32_t i = 0; i < 32; i++) {
    auto raw_next = (list *) pmemobj_direct(next);
    assert(raw_next->value == i);
    next = raw_next->next;
  }
  allocator.reset();
  remove(pool_name);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}