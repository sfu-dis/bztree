#include "bztree_pibench_wrapper.h"

#define TEST_LAYOUT_NAME "bztree_layout"

extern "C" tree_api *create_tree(const tree_options_t &opt) {
  return new bztree_wrapper(opt);
}

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

bztree::BzTree *create_new_tree(const tree_options_t &opt) {
  bztree::BzTree::ParameterSet param(1024, 512, 1024);

#ifdef PMDK
  pmwcas::InitLibrary(
      pmwcas::PMDKAllocator::Create(opt.pool_path.c_str(), "bztree_layout",
                                    opt.pool_size),
      pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto *bztree = reinterpret_cast<bztree::BzTree *>(
      pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  pmwcas::DescriptorPool *pool = nullptr;
  pmdk_allocator->Allocate((void **)&pool, sizeof(pmwcas::DescriptorPool));
  new (pool) pmwcas::DescriptorPool(100000, opt.num_threads, false);

  new (bztree) bztree::BzTree(
      param, pool, reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));
  pmdk_allocator->PersistPtr(bztree, sizeof(bztree::BzTree));
  pmdk_allocator->PersistPtr(pool, sizeof(pmwcas::DescriptorPool));
#else
  // Volatile variant
  // pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
  //                    pmwcas::DefaultAllocator::Destroy,
  pmwcas::InitLibrary(
      pmwcas::TlsAllocator::Create, pmwcas::TlsAllocator::Destroy,
      pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
  auto pool = new pmwcas::DescriptorPool(100000, opt.num_threads, false);
  auto *bztree = bztree::BzTree::New(param, pool);
#endif

  return bztree;
}

bztree::BzTree *recovery_from_pool(const tree_options_t &opt) {
  pmwcas::InitLibrary(
      pmwcas::PMDKAllocator::Create(opt.pool_path.c_str(), TEST_LAYOUT_NAME,
                                    opt.pool_size),
      pmwcas::PMDKAllocator::Destroy, pmwcas::LinuxEnvironment::Create,
      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto tree = reinterpret_cast<bztree::BzTree *>(
      pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  tree->Recovery();
  return tree;
}

bztree_wrapper::bztree_wrapper(const tree_options_t &opt) {
  if (FileExists(opt.pool_path.c_str())) {
    std::cout << "recovery from existing pool." << std::endl;
    tree_ = recovery_from_pool(opt);
  } else {
    std::cout << "creating new tree on pool." << std::endl;
    tree_ = create_new_tree(opt);
  }
}

bztree_wrapper::~bztree_wrapper() { pmwcas::Thread::ClearRegistry(); }

bool bztree_wrapper::find(const char *key, size_t key_sz, char *value_out) {
  // FIXME(tzwang): for now only support 8-byte values
  uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
  return tree_
      ->Read(reinterpret_cast<const char *>(&k), key_sz, (uint64_t *)value_out)
      .IsOk();
}

bool bztree_wrapper::insert(const char *key, size_t key_sz, const char *value,
                            size_t value_sz) {
  // FIXME(tzwang): for now only support 8-byte values
  assert(value_sz == sizeof(uint64_t));
  uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
  uint64_t v = *reinterpret_cast<uint64_t *>(const_cast<char *>(value));

  // Mask out the 3 MSBs
  v &= 0x1FFFFFFFFFFFFFFF;
  return tree_->Insert(reinterpret_cast<const char *>(&k), key_sz, v).IsOk();
}

bool bztree_wrapper::update(const char *key, size_t key_sz, const char *value,
                            size_t value_sz) {
  // FIXME(tzwang): for now only support 8-byte values
  assert(value_sz == sizeof(uint64_t));
  uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
  uint64_t v = *reinterpret_cast<uint64_t *>(const_cast<char *>(value));

  // Mask out the 3 MSBs
  v &= 0x1FFFFFFFFFFFFFFF;
  return tree_->Update(reinterpret_cast<const char *>(&k), key_sz, v).IsOk();
}

bool bztree_wrapper::remove(const char *key, size_t key_sz) {
  uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));
  return tree_->Delete(reinterpret_cast<const char *>(&k), key_sz).IsOk();
}

int bztree_wrapper::scan(const char *key, size_t key_sz, int scan_sz,
                         char *&values_out) {
  static thread_local std::array<char, (1 << 20)> results;
  uint64_t k = __builtin_bswap64(*reinterpret_cast<const uint64_t *>(key));

  int scanned = 0;
  char *dst = results.data();

  auto iter = tree_->RangeScanBySize(reinterpret_cast<const char *>(&k), key_sz,
                                     scan_sz);
  for (scanned = 0; (scanned < scan_sz); ++scanned) {
    auto record = iter->GetNext();
    if (record == nullptr) break;

    uint64_t result_key = __builtin_bswap64(
        *reinterpret_cast<const uint64_t *>(record->GetKey()));
    memcpy(dst, &result_key, sizeof(uint64_t));
    dst += sizeof(uint64_t);

    auto payload = record->GetPayload();
    memcpy(dst, &payload, sizeof(uint64_t));
    dst += sizeof(uint64_t);
  }
  values_out = results.data();
  return scanned;
}
