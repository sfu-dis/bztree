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
POBJ_LAYOUT_TOID(bztree, char);
POBJ_LAYOUT_END(bztree);

class Allocator {

 public:
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

  /*
   *  allocate size memory from pmem pool
   * */
  PMEMoid alloc(uint64_t size) {
    TOID(char) mem;
    POBJ_ALLOC(pop, &mem, char, sizeof(char) * size, NULL, NULL);
    if (TOID_IS_NULL(mem)) {
      LOG(FATAL) << "POBJ_ALLOC error" << std::endl;
    }
    pmemobj_persist(pop, D_RW(mem), size * sizeof(*D_RW(mem)));
    return mem.oid;
  }

  /*
   *  free a PM pointer
   * */
  void free(PMEMoid ptr) {
    TOID(char) ptr_cpy;
    TOID_ASSIGN(ptr_cpy, ptr);
    POBJ_FREE(&ptr_cpy);
  }

  inline PMEMobjpool *GetPool() { return pop; }

 private:
  PMEMobjpool *pop;
  Allocator(PMEMobjpool *pop) : pop(pop) {}
};
