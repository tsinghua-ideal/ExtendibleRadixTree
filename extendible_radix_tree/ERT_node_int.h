#ifndef NVMKV_ERT_NODE_INT_H
#define NVMKV_ERT_NODE_INT_H

#include <cstdint>
#include <sys/time.h>
#include <vector>
#include <map>
#include <math.h>
#include <cstdint>
#include "../fastalloc/fastalloc.h"

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))


#define GET_SEG_NUM(key, key_len, depth)  ((key>>(key_len-depth))&(((uint64_t)1<<depth)-1))
#define GET_BUCKET_NUM(key, bucket_mask_len) ((key)&(((uint64_t)1<<bucket_mask_len)-1))

#define GET_SEG_POS(currentNode,dir_index) (((uint64_t)(currentNode) + sizeof(ERTIntNode) + dir_index*sizeof(ERTIntNode*)))

#define GET_SUBKEY(key, start, length) ( (key>>(64 - start - length) & (((uint64_t)1<<length)-1)))

#define REMOVE_NODE_FLAG(key) (key & (((uint64_t)1<<56)-1) )
#define PUT_KEY_VALUE_FLAG(key) (key | ((uint64_t)1<<56))
#define GET_NODE_FLAG(key) (key>>56)

#define ERT_INIT_GLOBAL_DEPTH 0
#define ERT_BUCKET_SIZE 4
#define ERT_BUCKET_MASK_LEN 8
#define ERT_MAX_BUCKET_NUM (1<<ERT_BUCKET_MASK_LEN)

#define SIZE_OF_CHAR 8
#define ERT_NODE_LENGTH 32
#define ERT_NODE_PREFIX_MAX_BYTES 6
#define ERT_NODE_PREFIX_MAX_BITS 48
#define ERT_KEY_LENGTH 64

class ERTIntKeyValue {
public:
    uint64_t key = 0;// indeed only need uint8 or uint16
    uint64_t value = 0;

    void operator =(ERTIntKeyValue a){
        this->key = a.key;
        this->value = a.value;
    };
};

ERTIntKeyValue *NewERTIntKeyValue(uint64_t key, uint64_t value);

class ERTIntBucketKeyValue{
public:
    uint64_t subkey = 0;
    uint64_t value = 0;
};

class ERTIntBucket {
public:

    ERTIntBucketKeyValue counter[ERT_BUCKET_SIZE];

    uint64_t get(uint64_t key, bool& keyValueFlag);

    int findPlace(uint64_t _key, uint64_t _key_len, uint64_t _depth);
};

class ERTIntSegment {
public:
    uint64_t depth = 0;
    ERTIntBucket *bucket;
//    ERTIntBucket bucket[ERT_MAX_BUCKET_NUM];

    ERTIntSegment();

    ~ERTIntSegment();

    void init(uint64_t _depth);
};

ERTIntSegment *NewERTIntSegment(uint64_t _depth = 0);

class ERTIntHeader{
public:
    unsigned char len = 7;
    unsigned char depth;
    unsigned char array[6];

    void init(ERTIntHeader* oldHeader, unsigned char length, unsigned char depth);

    int computePrefix(uint64_t key, int pos);

    void assign(uint64_t key, int startPos);

    void assign(unsigned char* key, unsigned char assignedLength = ERT_NODE_PREFIX_MAX_BYTES);
};

class ERTIntNode {
public:
    ERTIntHeader header;
    unsigned char global_depth = 0;
    uint32_t dir_size = 1;
    ERTIntKeyValue* treeNodeValues;
    // used to represent the elements in the treenode prefix, but not in CCEH

    ERTIntNode();

    ~ERTIntNode();

    void init( unsigned char headerDepth = 0, unsigned char global_depth = 0);

    void put(uint64_t subkey, uint64_t value, uint64_t beforeAddress);

    void put(uint64_t subkey, uint64_t value, ERTIntSegment* tmp_seg, ERTIntBucket* tmp_bucket, uint64_t dir_index, uint64_t seg_index, uint64_t beforeAddress);

    void nodePut(int pos, ERTIntKeyValue *kv);

    uint64_t get(uint64_t subkey, bool& keyValueFlag);

};

ERTIntNode *NewERTIntNode(int _key_len, unsigned char headerDepth = 1,
                          unsigned char globalDepth = ERT_INIT_GLOBAL_DEPTH);


#endif //NVMKV_ERT_NODE_INT_H
