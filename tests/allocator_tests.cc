// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "../allocator.h"

TEST(AllocatorTest, MetaTest) {
  auto allocator = Allocator::New("test_pool");
  PMEMoid ptr = allocator->alloc(20);
  auto raw_ptr = pmemobj_direct(ptr);
  pmemobj_memcpy_persist(allocator->GetPool(), raw_ptr, "hello nvm!", 10);

  delete (allocator);

  auto new_allocator = Allocator::New("test_pool");
  raw_ptr = pmemobj_direct(ptr);
  std::cout << *reinterpret_cast<char *> (raw_ptr ) << std::endl;
}
