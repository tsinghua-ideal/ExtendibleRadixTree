//
// Created by 王柯 on 2021-04-01.
//

#ifndef NVMKV_WOwoart_H
#define NVMKV_WOwoart_H


#include <stdint.h>
#include <stdbool.h>
#include <vector>
#include "../fastalloc/fastalloc.h"

#ifdef __linux__
#include <byteswap.h>
#endif

#define NODE4        1
#define NODE16        2
#define NODE48        3
#define NODE256        4

#define WOART_BITS_PER_LONG        64
#define CACHE_LINE_SIZE    64

/* If you want to change the number of entries,
 * change the values of WOART_NODE_BITS & WOART_MAX_DEPTH */
#define WOART_NODE_BITS            8
#define WOART_MAX_DEPTH            7
#define WOART_NUM_NODE_ENTRIES    (0x1UL << WOART_NODE_BITS)
#define WOART_LOW_BIT_MASK        ((0x1UL << WOART_NODE_BITS) - 1)

#define WOART_MAX_PREFIX_LEN        6
#define WOART_MAX_HEIGHT            (WOART_MAX_DEPTH + 1)

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

static inline unsigned long __ffs(unsigned long word) {
    asm("rep; bsf %1,%0"
    : "=r" (word)
    : "rm" (word));
    return word;
}

static inline unsigned long ffz(unsigned long word) {
    asm("rep; bsf %1,%0"
    : "=r" (word)
    : "r" (~word));
    return word;
}

typedef int(*woart_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

extern uint64_t woart_memory_usage;

struct woart_key_value {
    uint64_t key;
    uint64_t value;
};

/**
 * path compression
 * pwoartial_len: Optimistic
 * pwoartial: Pessimistic
 */
typedef struct {
    unsigned char depth;
    unsigned char pwoartial_len;
    unsigned char pwoartial[WOART_MAX_PREFIX_LEN];
} path_comp;

/**
 * This struct is included as pwoart
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
    path_comp path;
} woart_node;

typedef struct {
    unsigned char key;
    char i_ptr;
} slot_array;

/**
 * Small node with only 4 children, but
 * 8byte slot array field.
 */
typedef struct {
    woart_node n;
    slot_array slot[4];
    woart_node *children[4];
} woart_node4;

/**
 * Node with 16 keys and 16 children, and
 * a 8byte bitmap field
 */
typedef struct {
    woart_node n;
    unsigned long bitmap;
    unsigned char keys[16];
    woart_node *children[16];
} woart_node16;

/**
 * Node with 48 children and a full 256 byte field,
 */
typedef struct {
    woart_node n;
    unsigned char keys[256];
    woart_node *children[48];
} woart_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    woart_node n;
    woart_node *children[256];
} woart_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;
    unsigned long key;
} woart_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    woart_node *root;
    uint64_t size;
} woart_tree;

/*
 * For range lookup in NODE16
 */
typedef struct {
    unsigned char key;
    woart_node *child;
} key_pos;

/**
 * Initializes an woart tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t);

woart_tree *new_woart_tree();

/**
 * DEPRECATED
 * Initializes an woart tree
 * @return 0 on success.
 */
#define init_woart_tree(...) woart_tree_init(__VA_ARGS__)

/**
 * Inserts a new value into the woart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void *woart_put(woart_tree *t, const unsigned long key, int key_len, void *value, int value_len = 8);

/**
 * Searches for a value in the woart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
uint64_t woart_get(const woart_tree *t, const unsigned long key, int key_len);

void woart_all_subtree_kv(woart_node *n, vector<woart_key_value> &res);

void woart_node_scan(woart_node *n, uint64_t left, uint64_t right, uint64_t depth, vector<woart_key_value> &res,
                     int key_len = 8);


vector<woart_key_value> woart_scan(const woart_tree *t, uint64_t left, uint64_t right, int key_len = 8);

uint64_t woart_memory_profile(woart_node *n);


#endif //NVMKV_WOwoart_H
