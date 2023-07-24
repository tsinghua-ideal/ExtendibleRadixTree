//
// Created by Guanqun Yang on 9/19/21.
//

#ifndef NVMKV_LBTREE_H
#define NVMKV_LBTREE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <thread>
#include <atomic>
#include <vector>
#include <map>
#include <immintrin.h>
#include <sys/time.h>
#include "nodeprof.h"
#include "../fastalloc/fastalloc.h"

//#define LB_PROFILE_TIME 1

#ifdef LB_PROFILE_TIME
extern timeval start_time, end_time;
extern uint64_t _grow, _update, _travelsal, _decompression;
#endif

//#define LB_SCAN_PROFILE_TIME 1

#ifdef LB_SCAN_PROFILE_TIME
extern timeval start_time, end_time;
extern uint64_t _random, _sequential;
#endif

#ifndef KB
#define KB      (1024)
#endif

#define NONLEAF_LINE_NUM        4    // 256B
#define LEAF_LINE_NUM           4    // 256B
#define CACHE_LINE_SIZE    64

#ifndef PREFETCH_NUM_AHEAD
#define PREFETCH_NUM_AHEAD    3
#endif

#define NON_LEAF_KEY_NUM    (NONLEAF_SIZE/(KEY_SIZE+POINTER_SIZE)-1)

#define NONLEAF_SIZE    (CACHE_LINE_SIZE * NONLEAF_LINE_NUM)
#define LEAF_SIZE       (CACHE_LINE_SIZE * LEAF_LINE_NUM)

#define LEAF_KEY_NUM        (14)

typedef long long key_type;
#define KEY_SIZE             8   /* size of a key in tree node */
#define POINTER_SIZE         8   /* size of a pointer/value in node */

#define bitScan(x)  __builtin_ffs(x)
#define countBit(x) __builtin_popcount(x)
#define ceiling(x, y)  (((x) + (y) - 1) / (y))
//#define max(x, y) ((x)<=(y) ? (y) : (x))
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

static inline unsigned char hashcode1B(key_type x) {
    x ^= x >> 32;
    x ^= x >> 16;
    x ^= x >> 8;
    return (unsigned char) (x & 0x0ffULL);
}


class Pointer8B {
public:
    unsigned long long value;  /* 8B to contain a pointer */

public:
    Pointer8B() {}

    Pointer8B(const void *ptr) { value = (unsigned long long) ptr; }

    Pointer8B(const Pointer8B &p) { value = p.value; }

    Pointer8B &operator=(const void *ptr) {
        value = (unsigned long long) ptr;
        return *this;
    }

    Pointer8B &operator=(const Pointer8B &p) {
        value = p.value;
        return *this;
    }

    bool operator==(const void *ptr) {
        bool result = (value == (unsigned long long) ptr);
        return result;
    }

    bool operator==(const Pointer8B &p) {
        bool result = (value == p.value);
        return result;
    }


    operator void *() { return (void *) value; }

    operator char *() { return (char *) value; }

    operator struct bnode *() { return (struct bnode *) value; }

    operator struct bleaf *() { return (struct bleaf *) value; }

    operator unsigned long long() { return value; }

    bool isNull(void) { return (value == 0); }

    void print(void) { printf("%llx\n", value); }

};

/**
 *  An IdxEntry consists of a key and a pointer.
 */
typedef struct IdxEntry {
    key_type k;
    Pointer8B ch;
} IdxEntry;


typedef struct bnodeMeta {
    int lock;    /* lock bit for concurrency control */
    int num;     /* number of keys */
} bnodeMeta;

class bnode {
public:
    IdxEntry ent[NON_LEAF_KEY_NUM + 1];
public:
    key_type &k(int idx) { return ent[idx].k; }

    Pointer8B &ch(int idx) { return ent[idx].ch; }

    char *chEndAddr(int idx) {
        return (char *) &(ent[idx].ch) + sizeof(Pointer8B) - 1;
    }

    int &num(void) { return ((bnodeMeta *) &(ent[0].k))->num; }

    int &lock(void) { return ((bnodeMeta *) &(ent[0].k))->lock; }

    int tryLock(){
        int noLock = 0;
        if(!CAS(&lock(),&noLock,1)){
            return 1;
        }else{
            return 1;
        }
    }

    void unlock(){
        lock() = 0;
    }
}; // bnode

typedef union bleafMeta {
    unsigned long long word8B[2];
    struct {
        uint16_t bitmap: 14;
        uint16_t lock: 1;
        uint16_t alt: 1;
        unsigned char fgpt[LEAF_KEY_NUM]; /* fingerprints */
    } v;
} bleafMeta;


class bleaf {
public:
    uint16_t bitmap: 14;
    uint16_t lock: 1;
    uint16_t alt: 1;
    unsigned char fgpt[LEAF_KEY_NUM]; /* fingerprints */
    IdxEntry ent[LEAF_KEY_NUM];
    bleaf *next[2];

public:
    key_type &k(int idx) { return ent[idx].k; }

    Pointer8B &ch(int idx) { return ent[idx].ch; }

    int num() { return countBit(bitmap); }

    bleaf *nextSibling() { return next[alt]; }

    bool isFull(void) { return (bitmap == 0x3fff); }

    void setBothWords(bleafMeta *m) {
        bleafMeta *my_meta = (bleafMeta *) this;
        my_meta->word8B[1] = m->word8B[1];
        my_meta->word8B[0] = m->word8B[0];
    }

    void setWord0(bleafMeta *m) {
        bleafMeta *my_meta = (bleafMeta *) this;
        my_meta->word8B[0] = m->word8B[0];
    }

    int tryLock(){
        if(lock == 1 ){
            return 0;
        }else{
            lock = 1;
            return 1;
        }
    }

    void unlock(){
        lock = 0;
    }

}; // bleaf

class treeMeta {
public:
    int root_level; // leaf: level 0, parent of leaf: level 1
    Pointer8B tree_root;
    bleaf **first_leaf; // on NVM

public:
    treeMeta(void *nvm_address, bool recover = false) {
        root_level = 0;
        tree_root = NULL;
        first_leaf = (bleaf **) nvm_address;

        if (!recover) setFirstLeaf(NULL);
    }

    void init(void *nvm_address) {
        root_level = 0;
        tree_root = NULL;
        first_leaf = (bleaf **) nvm_address;
    }

    void setFirstLeaf(bleaf *leaf);
};


class lbtree {
public:
    treeMeta *tree_meta;

public:

    uint64_t memory_usage = 0;

    class kv{
    public:
        key_type k;
        uint64_t v;
    };

    int bulkload(int keynum, key_type input, float bfill = 1.0);

    int bulkloadSubtree(key_type input, int start_key, int num_key, float bfill, int target_level, Pointer8B pfirst[], int n_nodes[]);

    void init();

    void *lookup(key_type key, int *pos=0);

    void *get_recptr(void *p, int pos) {
        return ((bleaf *) p)->ch(pos);
    }

    // insert (key, ptr)
    void insert(key_type key, void *_ptr);

    // delete key
    void del(key_type key);

    int level() { return tree_meta->root_level; }

    vector<kv> rangeQuery(key_type key , key_type end);

    uint64_t memory_profile();

private:

    void qsortBleaf(bleaf *p, int start, int end, int pos[]);
};

lbtree *new_lbtree();

#endif
