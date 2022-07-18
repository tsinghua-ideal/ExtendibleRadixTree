#ifndef NVMKV_ERT_INT_H
#define NVMKV_ERT_INT_H

#include "ERT_node_int.h"

class ERTInt {
public:
    int init_depth = 0; //represent extendible hash initial global depth
    ERTIntNode *root = NULL;

    ERTInt();

    ERTInt(int _span, int _init_depth);

    ~ERTInt();

    void init();

    //support variable values, for convenience, we set v to 8 byte
    void Insert(uint64_t key, uint64_t value, ERTIntNode *_node = NULL, int len = 0);

    uint64_t Search(uint64_t key);

    void scan(uint64_t left, uint64_t right);

    void nodeScan(ERTIntNode *tmp, uint64_t left, uint64_t right, vector<ERTIntKeyValue> &res, int pos = 0,
                  uint64_t prefix = 0);

    void getAllNodes(ERTIntNode *tmp, vector<ERTIntKeyValue> &res, int pos = 0, uint64_t prefix = 0);

    uint64_t memory_profile(ERTIntNode *tmp, int pos = 0);
};

ERTInt *NewExtendibleRadixTreeInt();

#endif //NVMKV_ERT_INT_H
