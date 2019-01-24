// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#pragma once
#include <sys/stat.h>
#include <libpmemobj.h>

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

POBJ_LAYOUT_BEGIN(bztree);
POBJ_LAYOUT_TOID(bztree, int);
POBJ_LAYOUT_END(bztree);

class Allocator {

  Allocator(PMEMobjpool *pop) : pop(pop) {}

  static bool FileExists(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
  }

  static Allocator *New(const char *pool_path) {
    PMEMobjpool *tmp_pool;
    if (FileExists(pool_path)) {
      tmp_pool = pmemobj_create(pool_path, POBJ_LAYOUT_NAME(bztree),
                                PMEMOBJ_MIN_POOL, CREATE_MODE_RW);

      LOG_ASSERT(tmp_pool != NULL);
    } else {
      tmp_pool = pmemobj_open(pool_path, POBJ_LAYOUT_NAME(bztree));
      LOG_ASSERT(tmp_pool != NULL);
    }
    return new Allocator(tmp_pool);
  }

 private:
  PMEMobjpool *pop;
};
