// Copyright (c) Simon Fraser University
//
// Authors:
// Tianzheng Wang <tzwang@sfu.ca>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>

#pragma once
#include <sys/stat.h>
#include <libpmemobj.h>
#include <stdio.h>

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

  explicit Allocator(PMEMobjpool *pop, const char *file_name) : pop(pop), file_name(file_name) {}

  static std::unique_ptr<Allocator> New(const char *pool_path, const char *layout) {
    PMEMobjpool *tmp_pool;
    if (!FileExists(pool_path)) {
      tmp_pool = pmemobj_create(pool_path, layout,
                                PMEMOBJ_MIN_POOL, CREATE_MODE_RW);

      LOG_ASSERT(tmp_pool != nullptr);
    } else {
      tmp_pool = pmemobj_open(pool_path, layout);
      LOG_ASSERT(tmp_pool != nullptr);
    }
    return std::make_unique<Allocator>(tmp_pool, pool_path);
  }

  /*
   *  allocate size memory from pmem pool
   *  return a pmemoid
   * */
  PMEMoid Alloc(uint64_t size, bool zero_out = false) {
    TOID(char) mem;
    if (zero_out) {
      POBJ_ZALLOC(pop, &mem, char, sizeof(char) * size);
    } else {
      POBJ_ALLOC(pop, &mem, char, sizeof(char) * size, NULL, NULL);
    }
    if (TOID_IS_NULL(mem)) {
      LOG(FATAL) << "POBJ_ALLOC error" << std::endl;
    }
    pmemobj_persist(pop, D_RW(mem), size * sizeof(*D_RW(mem)));
    return mem.oid;
  }

  /*
   *  allocate size memory from pmem pool
   *  return the direct pointer
   * */
  void *AllocDirect(uint64_t size, bool zero_out = false) {
    return pmemobj_direct(Alloc(size, zero_out));
  }

  /*
   *  free a PM pointer
   * */
  void Free(PMEMoid ptr) {
    TOID(char) ptr_cpy;
    TOID_ASSIGN(ptr_cpy, ptr);
    POBJ_FREE(&ptr_cpy);
  }

  inline PMEMoid GetRoot(uint64_t size) {
    return pmemobj_root(GetPool(), size);
  }

  inline void *GetDirectRoot(uint64_t size) {
    return pmemobj_direct(GetRoot(size));
  }

  inline void PersistPtr(void *ptr, uint64_t size) {
    pmemobj_persist(pop, ptr, size);
  }

  inline PMEMobjpool *GetPool() { return pop; }

  inline void ClosePool() {
    pmemobj_close(pop);
  }

  ~Allocator() {
    LOG(INFO) << "allocator destroyed, closing the pool";
    pmemobj_close(pop);
  }

 private:
  PMEMobjpool *pop;
  const char *file_name;
};
