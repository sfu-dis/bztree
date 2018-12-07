#pragma once
#include "mwcas/mwcas.h"

namespace bztree {

struct NodeHeader {
  // Header:
  // |-------64 bits-------|---32 bits---|---32 bits---|
  // |     status word     |     size    | sorted count|
  //
  // Sorted count is actually the index into the next available metadata array
  // entry. Following the header is a growing array of record metadata entries.
  uint64_t status;
  uint32_t size;
  uint32_t sorted_count;
  NodeHeader() : status(0), size(0), sorted_count(0) {}
};

class InternalNode {
public:
  static const uint32_t kNodeSize = 4096;

private:
  NodeHeader header;
  uint64_t record_metadata[0];

public:
  static InternalNode *New();

  InternalNode() {}
  ~InternalNode() {}
};

class LeafNode {
public:
  static const uint32_t kNodeSize = 4096;

private:
  NodeHeader header;

public:
  static LeafNode *New();

  LeafNode() {}
  ~LeafNode() {}
};

class BzTree {
public:
  BzTree() : epoch(0) {}
private:
  uint64_t epoch;
};



}  // namespace bztree
