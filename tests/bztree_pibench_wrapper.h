#pragma once
// A wrapper for the Bz-Tree

#include "tree_api.hpp"
#include "../bztree.h"

class bztree_wrapper : public tree_api
{
public:
    bztree_wrapper(const tree_options_t &opt);
    virtual ~bztree_wrapper();

    virtual bool find(const char *key, size_t key_sz, char *value_out) override;
    virtual bool insert(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool update(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
    virtual bool remove(const char *key, size_t key_sz) override;
    virtual int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) override;

    bool recovery(const tree_options_t &opt);

private:
    bztree::BzTree *tree_;
};
