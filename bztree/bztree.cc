#include "bztree.h"

namespace bztree {

InternalNode *InternalNode::New() {
  // FIXME(tzwang): use a better allocator
  InternalNode *node = (InternalNode *)malloc(InternalNode::kNodeSize);
  new (node) InternalNode;
  return node;
}

}  // namespace bztree
