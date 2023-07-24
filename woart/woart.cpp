//
// Created by 王柯 on 2021-04-01.
//

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <emmintrin.h>
#include <assert.h>
#include <x86intrin.h>
#include "woart.h"

#define mfence() asm volatile("mfence":::"memory")
#define BITOP_WORD(nr)    ((nr) / WOART_BITS_PER_LONG)

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uintptr_t)x & 1))
#define SET_LEAF(x) ((void*)((uintptr_t)x | 1))
#define LEAF_RAW(x) ((woart_leaf*)((void*)((uintptr_t)x & ~1)))

uint64_t woart_memory_usage = 0;


void flush_buffer(void *buf, unsigned long len, bool fence) {
    unsigned long i;
    len = len + ((unsigned long) (buf) & (CACHE_LINE_SIZE - 1));
    if (fence) {
        mfence();
        for (i = 0; i < len; i += CACHE_LINE_SIZE) {
            asm volatile ("clflush %0\n" : "+m" (*((char *) buf + i)));
        }
        mfence();
    } else {
        for (i = 0; i < len; i += CACHE_LINE_SIZE) {
            asm volatile ("clflush %0\n" : "+m" (*((char *) buf + i)));
        }
    }
}

static int get_index(unsigned long key, int depth) {
    int index;

    index = ((key >> ((WOART_MAX_DEPTH - depth) * WOART_NODE_BITS)) & WOART_LOW_BIT_MASK);
    return index;
}

/*
 * Find the next set bit in a memory region.
 */
unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
                            unsigned long offset) {
    const unsigned long *p = addr + BITOP_WORD(offset);
    unsigned long result = offset & ~(WOART_BITS_PER_LONG - 1);
    unsigned long tmp;

    if (offset >= size)
        return size;
    size -= result;
    offset %= WOART_BITS_PER_LONG;
    if (offset) {
        tmp = *(p++);
        tmp &= (~0UL << offset);
        if (size < WOART_BITS_PER_LONG)
            goto found_first;
        if (tmp)
            goto found_middle;
        size -= WOART_BITS_PER_LONG;
        result += WOART_BITS_PER_LONG;
    }
    while (size & ~(WOART_BITS_PER_LONG - 1)) {
        if ((tmp = *(p++)))
            goto found_middle;
        result += WOART_BITS_PER_LONG;
        size -= WOART_BITS_PER_LONG;
    }
    if (!size)
        return result;
    tmp = *p;

    found_first:
    tmp &= (~0UL >> (WOART_BITS_PER_LONG - size));
    if (tmp == 0UL)        /* Are any bits set? */
        return result + size;    /* Nope. */
    found_middle:
    return result + __ffs(tmp);
}

/*
 * Find the next zero bit in a memory region
 */
unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
                                 unsigned long offset) {
    const unsigned long *p = addr + BITOP_WORD(offset);
    unsigned long result = offset & ~(WOART_BITS_PER_LONG - 1);
    unsigned long tmp;

    if (offset >= size)
        return size;
    size -= result;
    offset %= WOART_BITS_PER_LONG;
    if (offset) {
        tmp = *(p++);
        tmp |= ~0UL >> (WOART_BITS_PER_LONG - offset);
        if (size < WOART_BITS_PER_LONG)
            goto found_first;
        if (~tmp)
            goto found_middle;
        size -= WOART_BITS_PER_LONG;
        result += WOART_BITS_PER_LONG;
    }
    while (size & ~(WOART_BITS_PER_LONG - 1)) {
        if (~(tmp = *(p++)))
            goto found_middle;
        result += WOART_BITS_PER_LONG;
        size -= WOART_BITS_PER_LONG;
    }
    if (!size)
        return result;
    tmp = *p;

    found_first:
    tmp |= ~0UL << size;
    if (tmp == ~0UL)
        return result + size;
    found_middle:
    return result + ffz(tmp);
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
static woart_node *alloc_node(uint8_t type) {
    woart_node *n;
    void *ret;
    int i;
    switch (type) {
        case NODE4:
            ret = concurrency_fast_alloc(sizeof(woart_node4));
            woart_memory_usage += sizeof(woart_node4) - sizeof(woart_leaf);
//            posix_memalign(&ret, 64, sizeof(woart_node4));
            n = static_cast<woart_node *>(ret);
            for (i = 0; i < 4; i++)
                ((woart_node4 *) n)->slot[i].i_ptr = -1;
            break;
        case NODE16:
            ret = concurrency_fast_alloc(sizeof(woart_node16));
            woart_memory_usage += sizeof(woart_node16) - sizeof(woart_node4);
//            posix_memalign(&ret, 64, sizeof(woart_node16));
            n = static_cast<woart_node *>(ret);
            ((woart_node16 *) n)->bitmap = 0;
            break;
        case NODE48:
            ret = concurrency_fast_alloc(sizeof(woart_node48));
            woart_memory_usage += sizeof(woart_node48) - sizeof(woart_node16);
//            posix_memalign(&ret, 64, sizeof(woart_node48));
            n = static_cast<woart_node *>(ret);
            memset(n, 0, sizeof(woart_node48));
            break;
        case NODE256:
            ret = concurrency_fast_alloc(sizeof(woart_node256));
            woart_memory_usage += sizeof(woart_node256) - sizeof(woart_node48);
//            posix_memalign(&ret, 64, sizeof(woart_node256));
            n = static_cast<woart_node *>(ret);
            memset(n, 0, sizeof(woart_node256));
            break;
        default:
            abort();
    }
    n->type = type;
    return n;
}

/**
 * Initializes an woart tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t) {
    t->root = NULL;
    t->size = 0;
    return 0;
}

woart_tree *new_woart_tree() {
    woart_tree *_new_woart_tree = static_cast<woart_tree *>(concurrency_fast_alloc(sizeof(woart_tree)));
    woart_memory_usage += sizeof(woart_tree);
    woart_tree_init(_new_woart_tree);
    return _new_woart_tree;
}

static woart_node **find_child(woart_node *n, unsigned char c) {
    int i;
    union {
        woart_node4 *p1;
        woart_node16 *p2;
        woart_node48 *p3;
        woart_node256 *p4;
    } p;
    switch (n->type) {
        case NODE4:
            p.p1 = (woart_node4 *) n;
            for (i = 0; (i < 4 && (p.p1->slot[i].i_ptr != -1)); i++) {
                if (p.p1->slot[i].key == c)
                    return &p.p1->children[p.p1->slot[i].i_ptr];
            }
            break;
        case NODE16:
            p.p2 = (woart_node16 *) n;
            for (i = 0; i < 16; i++) {
                i = find_next_bit(&p.p2->bitmap, 16, i);
                if (i < 16 && p.p2->keys[i] == c)
                    return &p.p2->children[i];
            }
            break;
        case NODE48:
            p.p3 = (woart_node48 *) n;
            i = p.p3->keys[c];
            if (i)
                return &p.p3->children[i - 1];
            break;
        case NODE256:
            p.p4 = (woart_node256 *) n;
            if (p.p4->children[c])
                return &p.p4->children[c];
            break;
        default:
            abort();
    }
    return NULL;
}

// Simple inlined if
static inline int min(int a, int b) {
    return (a < b) ? a : b;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
static int check_prefix(const woart_node *n, const unsigned long key, int key_len, int depth) {
//	int max_cmp = min(min(n->pwoartial_len, WOART_MAX_PREFIX_LEN), (key_len * INDEX_BITS) - depth);
    int max_cmp = min(min(n->path.pwoartial_len, WOART_MAX_PREFIX_LEN), WOART_MAX_HEIGHT - depth);
    int idx;
    for (idx = 0; idx < max_cmp; idx++) {
        if (n->path.pwoartial[idx] != get_index(key, depth + idx))
            return idx;
    }
    return idx;
}

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
static int leaf_matches(const woart_leaf *n, unsigned long key, int key_len, int depth) {
    (void) depth;
    // Fail if the key lengths are different
    if (n->key_len != (uint32_t) key_len) return 1;

    // Compare the keys stwoarting at the depth
//	return memcmp(n->key, key, key_len);
    return !(n->key == key);
}

/**
 * Searches for a value in the woart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
uint64_t woart_get(const woart_tree *t, const unsigned long key, int key_len) {
    woart_node **child;
    woart_node *n = t->root;
    int prefix_len, depth = 0;

    while (n) {
        // Might be a leaf
        if (IS_LEAF(n)) {
            n = (woart_node *) LEAF_RAW(n);
            // Check if the expanded path matches
            if (!leaf_matches((woart_leaf *) n, key, key_len, depth)) {
                return *(uint64_t *)((woart_leaf *) n)->value;
            }
            return NULL;
        }

        if (n->path.depth == depth) {
            // Bail if the prefix does not match
            if (n->path.pwoartial_len) {
                prefix_len = check_prefix(n, key, key_len, depth);
                if (prefix_len != min(WOART_MAX_PREFIX_LEN, n->path.pwoartial_len))
                    return NULL;
                depth = depth + n->path.pwoartial_len;
            }
        } else {
            printf("Search: Crash occured\n");
            exit(0);
        }

        // Recursively search
        child = find_child(n, get_index(key, depth));
        n = (child) ? *child : NULL;
        depth++;
    }
    return NULL;
}

// Find the minimum leaf under a node
static woart_leaf *minimum(const woart_node *n) {
    // Handle base cases
    if (!n) return NULL;
    if (IS_LEAF(n)) return LEAF_RAW(n);

    int i, j, idx, min;
    switch (n->type) {
        case NODE4:
            return minimum(((woart_node4 *) n)->children[((woart_node4 *) n)->slot[0].i_ptr]);
        case NODE16:
            i = find_next_bit(&((woart_node16 *) n)->bitmap, 16, 0);
            min = ((woart_node16 *) n)->keys[i];
            idx = i;
            for (i = i + 1; i < 16; i++) {
                i = find_next_bit(&((woart_node16 *) n)->bitmap, 16, i);
                if (((woart_node16 *) n)->keys[i] < min && i < 16) {
                    min = ((woart_node16 *) n)->keys[i];
                    idx = i;
                }
            }
            return minimum(((woart_node16 *) n)->children[idx]);
        case NODE48:
            idx = 0;
            while (!((woart_node48 *) n)->keys[idx]) idx++;
            idx = ((woart_node48 *) n)->keys[idx] - 1;
            return minimum(((woart_node48 *) n)->children[idx]);
        case NODE256:
            idx = 0;
            while (!((woart_node256 *) n)->children[idx]) idx++;
            return minimum(((woart_node256 *) n)->children[idx]);
        default:
            abort();
    }
}

static woart_leaf *make_leaf(const unsigned long key, int key_len, void *value, bool flush) {
    //woart_leaf *l = (woart_leaf*)malloc(sizeof(woart_leaf));
    woart_leaf *l;
    void *ret;
    ret = concurrency_fast_alloc(sizeof(woart_leaf));
    woart_memory_usage += sizeof(woart_leaf);
//    posix_memalign(&ret, 64, sizeof(woart_leaf));
    l = static_cast<woart_leaf *>(ret);
    l->value = value;
    l->key_len = key_len;
    l->key = key;

    if (flush == true)
        flush_buffer(l, sizeof(woart_leaf), true);
    return l;
}

static int longest_common_prefix(woart_leaf *l1, woart_leaf *l2, int depth) {
//	int idx, max_cmp = (min(l1->key_len, l2->key_len) * INDEX_BITS) - depth;
    int idx, max_cmp = WOART_MAX_HEIGHT - depth;

    for (idx = 0; idx < max_cmp; idx++) {
        if (get_index(l1->key, depth + idx) != get_index(l2->key, depth + idx))
            return idx;
    }
    return idx;
}

static void copy_header(woart_node *dest, woart_node *src) {
    memcpy(&dest->path, &src->path, sizeof(path_comp));
}

static void add_child256(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
    (void) ref;
    n->children[c] = (woart_node *) child;
    flush_buffer(&n->children[c], 8, true);
}

static void add_child256_noflush(woart_node256 *n, woart_node **ref, unsigned char c, void *child) {
    (void) ref;
    n->children[c] = (woart_node *) child;
}

static void add_child48(woart_node48 *n, woart_node **ref, unsigned char c, void *child) {
    unsigned long bitmap = 0;
    int i, num = 0;

    for (i = 0; i < 256; i++) {
        if (n->keys[i]) {
            bitmap += (0x1UL << (n->keys[i] - 1));
            num++;
            if (num == 48)
                break;
        }
    }

    if (num < 48) {
        unsigned long pos = find_next_zero_bit(&bitmap, 48, 0);
        n->children[pos] = (woart_node *) child;
        flush_buffer(&n->children[pos], 8, true);
        n->keys[c] = pos + 1;
        flush_buffer(&n->keys[c], sizeof(unsigned char), true);
    } else {
        woart_node256 *new_node = (woart_node256 *) alloc_node(NODE256);
        for (i = 0; i < 256; i++) {
            if (n->keys[i]) {
                new_node->children[i] = n->children[n->keys[i] - 1];
                num--;
                if (num == 0)
                    break;
            }
        }
        copy_header((woart_node *) new_node, (woart_node *) n);
        add_child256_noflush(new_node, ref, c, child);
        flush_buffer(new_node, sizeof(woart_node256), true);

        *ref = (woart_node *) new_node;
        flush_buffer(ref, 8, true);

//        free(n);
    }
}

static void add_child16(woart_node16 *n, woart_node **ref, unsigned char c, void *child) {
    if (n->bitmap != ((0x1UL << 16) - 1)) {
        int empty_idx;

        empty_idx = find_next_zero_bit(&n->bitmap, 16, 0);
        if (empty_idx == 16) {
            printf("find next zero bit error add_child16\n");
            abort();
        }

        n->keys[empty_idx] = c;
        n->children[empty_idx] = static_cast<woart_node *>(child);
        mfence();
        flush_buffer(&n->keys[empty_idx], sizeof(unsigned char), false);
        flush_buffer(&n->children[empty_idx], sizeof(uintptr_t), false);
        mfence();

        n->bitmap += (0x1UL << empty_idx);
        flush_buffer(&n->bitmap, sizeof(unsigned long), true);
    } else {
        int idx;
        woart_node48 *new_node = (woart_node48 *) alloc_node(NODE48);

        memcpy(new_node->children, n->children,
               sizeof(void *) * 16);
        for (idx = 0; idx < 16; idx++) {
            new_node->keys[n->keys[idx]] = idx + 1;
        }
        copy_header((woart_node *) new_node, (woart_node *) n);

        new_node->keys[c] = 17;
        new_node->children[16] = static_cast<woart_node *>(child);
        flush_buffer(new_node, sizeof(woart_node48), true);

        *ref = (woart_node *) new_node;
        flush_buffer(ref, sizeof(uintptr_t), true);

//        free(n);
    }
}

static void add_child4(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
    if (n->slot[3].i_ptr == -1) {
        slot_array temp_slot[4];
        int i, idx, mid = -1;
        unsigned long p_idx = 0;

        for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
            p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
            if (mid == -1 && c < n->slot[idx].key)
                mid = idx;
        }

        if (mid == -1)
            mid = idx;

        p_idx = find_next_zero_bit(&p_idx, 4, 0);
        if (p_idx == 4) {
            printf("find next zero bit error in child4\n");
            abort();
        }
        n->children[p_idx] = static_cast<woart_node *>(child);
        flush_buffer(&n->children[p_idx], sizeof(uintptr_t), true);

        for (i = idx - 1; i >= mid; i--) {
            temp_slot[i + 1].key = n->slot[i].key;
            temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
        }

        if (idx < 3) {
            for (i = idx + 1; i < 4; i++)
                temp_slot[i].i_ptr = -1;
        }

        temp_slot[mid].key = c;
        temp_slot[mid].i_ptr = p_idx;

        for (i = mid - 1; i >= 0; i--) {
            temp_slot[i].key = n->slot[i].key;
            temp_slot[i].i_ptr = n->slot[i].i_ptr;
        }

        *((uint64_t *) n->slot) = *((uint64_t *) temp_slot);
        flush_buffer(n->slot, sizeof(uintptr_t), true);
    } else {
        int idx;
        woart_node16 *new_node = (woart_node16 *) alloc_node(NODE16);

        for (idx = 0; idx < 4; idx++) {
            new_node->keys[n->slot[idx].i_ptr] = n->slot[idx].key;
            new_node->children[n->slot[idx].i_ptr] = n->children[n->slot[idx].i_ptr];
            new_node->bitmap += (0x1UL << n->slot[idx].i_ptr);
        }
        copy_header((woart_node *) new_node, (woart_node *) n);

        new_node->keys[4] = c;
        new_node->children[4] = static_cast<woart_node *>(child);
        new_node->bitmap += (0x1UL << 4);
        flush_buffer(new_node, sizeof(woart_node16), true);

        *ref = (woart_node *) new_node;
        flush_buffer(ref, 8, true);

//        free(n);
    }
}

static void add_child4_noflush(woart_node4 *n, woart_node **ref, unsigned char c, void *child) {
    slot_array temp_slot[4];
    int i, idx, mid = -1;
    unsigned long p_idx = 0;

    for (idx = 0; (idx < 4 && (n->slot[idx].i_ptr != -1)); idx++) {
        p_idx = p_idx + (0x1UL << n->slot[idx].i_ptr);
        if (mid == -1 && c < n->slot[idx].key)
            mid = idx;
    }

    if (mid == -1)
        mid = idx;

    p_idx = find_next_zero_bit(&p_idx, 4, 0);
    if (p_idx == 4) {
        printf("find next zero bit error in child4\n");
        abort();
    }

    n->children[p_idx] = static_cast<woart_node *>(child);

    for (i = idx - 1; i >= mid; i--) {
        temp_slot[i + 1].key = n->slot[i].key;
        temp_slot[i + 1].i_ptr = n->slot[i].i_ptr;
    }

    if (idx < 3) {
        for (i = idx + 1; i < 4; i++)
            temp_slot[i].i_ptr = -1;
    }

    temp_slot[mid].key = c;
    temp_slot[mid].i_ptr = p_idx;

    for (i = mid - 1; i >= 0; i--) {
        temp_slot[i].key = n->slot[i].key;
        temp_slot[i].i_ptr = n->slot[i].i_ptr;
    }

    *((uint64_t *) n->slot) = *((uint64_t *) temp_slot);
}

static void add_child(woart_node *n, woart_node **ref, unsigned char c, void *child) {
    switch (n->type) {
        case NODE4:
            return add_child4((woart_node4 *) n, ref, c, child);
        case NODE16:
            return add_child16((woart_node16 *) n, ref, c, child);
        case NODE48:
            return add_child48((woart_node48 *) n, ref, c, child);
        case NODE256:
            return add_child256((woart_node256 *) n, ref, c, child);
        default:
            abort();
    }
}

/**
 * Calculates the index at which the prefixes mismatch
 */
static int prefix_mismatch(const woart_node *n, const unsigned long key, int key_len, int depth, woart_leaf **l) {
//	int max_cmp = min(min(WOART_MAX_PREFIX_LEN, n->pwoartial_len), (key_len * INDEX_BITS) - depth);
    int max_cmp = min(min(WOART_MAX_PREFIX_LEN, n->path.pwoartial_len), WOART_MAX_HEIGHT - depth);
    int idx;
    for (idx = 0; idx < max_cmp; idx++) {
        if (n->path.pwoartial[idx] != get_index(key, depth + idx))
            return idx;
    }

    // If the prefix is short we can avoid finding a leaf
    if (n->path.pwoartial_len > WOART_MAX_PREFIX_LEN) {
        // Prefix is longer than what we've checked, find a leaf
        *l = minimum(n);
//		max_cmp = (min((*l)->key_len, key_len) * INDEX_BITS) - depth;
        max_cmp = WOART_MAX_HEIGHT - depth;
        for (; idx < max_cmp; idx++) {
            if (get_index((*l)->key, idx + depth) != get_index(key, depth + idx))
                return idx;
        }
    }
    return idx;
}

static void *recursive_insert(woart_node *n, woart_node **ref, const unsigned long key,
                              int key_len, void *value, int depth, int *old) {
    // If we are at a NULL node, inject a leaf
    if (!n) {
        *ref = (woart_node *) SET_LEAF(make_leaf(key, key_len, value, true));
        flush_buffer(ref, sizeof(uintptr_t), true);
        return NULL;
    }

    // If we are at a leaf, we need to replace it with a node
    if (IS_LEAF(n)) {
        woart_leaf *l = LEAF_RAW(n);

        // Check if we are updating an existing value
        if (!leaf_matches(l, key, key_len, depth)) {
            *old = 1;
            void *old_val = l->value;
            l->value = value;
            flush_buffer(&l->value, sizeof(uintptr_t), true);
            return old_val;
        }

        // New value, we must split the leaf into a node4
        woart_node4 *new_node = (woart_node4 *) alloc_node(NODE4);
        new_node->n.path.depth = depth;

        // Create a new leaf
        woart_leaf *l2 = make_leaf(key, key_len, value, false);

        // Determine longest prefix
        int i, longest_prefix = longest_common_prefix(l, l2, depth);
        new_node->n.path.pwoartial_len = longest_prefix;
        for (i = 0; i < min(WOART_MAX_PREFIX_LEN, longest_prefix); i++)
            new_node->n.path.pwoartial[i] = get_index(key, depth + i);

        add_child4_noflush(new_node, ref, get_index(l->key, depth + longest_prefix), SET_LEAF(l));
        add_child4_noflush(new_node, ref, get_index(l2->key, depth + longest_prefix), SET_LEAF(l2));

        mfence();
        flush_buffer(new_node, sizeof(woart_node4), false);
        flush_buffer(l2, sizeof(woart_leaf), false);
        mfence();

        // Add the leafs to the new node4
        *ref = (woart_node *) new_node;
        flush_buffer(ref, sizeof(uintptr_t), true);
        return NULL;
    }

    if (n->path.depth != depth) {
        printf("Insert: system is previously crashed!!\n");
        exit(0);
    }

    // Check if given node has a prefix
    if (n->path.pwoartial_len) {
        // Determine if the prefixes differ, since we need to split
        woart_leaf *l = NULL;
        int prefix_diff = prefix_mismatch(n, key, key_len, depth, &l);
        if ((uint32_t) prefix_diff >= n->path.pwoartial_len) {
            depth += n->path.pwoartial_len;
            goto RECURSE_SEARCH;
        }

        // Create a new node
        woart_node4 *new_node = (woart_node4 *) alloc_node(NODE4);
        new_node->n.path.depth = depth;
        new_node->n.path.pwoartial_len = prefix_diff;
        memcpy(new_node->n.path.pwoartial, n->path.pwoartial, min(WOART_MAX_PREFIX_LEN, prefix_diff));

        // Adjust the prefix of the old node
        path_comp temp_path;
        if (n->path.pwoartial_len <= WOART_MAX_PREFIX_LEN) {
            add_child4_noflush(new_node, ref, n->path.pwoartial[prefix_diff], n);
            temp_path.pwoartial_len = n->path.pwoartial_len - (prefix_diff + 1);
            temp_path.depth = (depth + prefix_diff + 1);
            memmove(temp_path.pwoartial, n->path.pwoartial + prefix_diff + 1,
                    min(WOART_MAX_PREFIX_LEN, temp_path.pwoartial_len));
        } else {
            int i;
            if (l == NULL)
                l = minimum(n);
            add_child4_noflush(new_node, ref, get_index(l->key, depth + prefix_diff), n);
            temp_path.pwoartial_len = n->path.pwoartial_len - (prefix_diff + 1);
            for (i = 0; i < min(WOART_MAX_PREFIX_LEN, temp_path.pwoartial_len); i++)
                temp_path.pwoartial[i] = get_index(l->key, depth + prefix_diff + 1 + i);
            temp_path.depth = (depth + prefix_diff + 1);
        }

        // Insert the new leaf
        l = make_leaf(key, key_len, value, false);
        add_child4_noflush(new_node, ref, get_index(key, depth + prefix_diff), SET_LEAF(l));

        mfence();
        flush_buffer(new_node, sizeof(woart_node4), false);
        flush_buffer(l, sizeof(woart_leaf), false);
        mfence();

        *ref = (woart_node *) new_node;
        *((uint64_t *) &n->path) = *((uint64_t *) &temp_path);

        mfence();
        flush_buffer(&n->path, sizeof(path_comp), false);
        flush_buffer(ref, sizeof(uintptr_t), false);
        mfence();

        return NULL;
    }

    RECURSE_SEARCH:;

    // Find a child to recurse to
    woart_node **child = find_child(n, get_index(key, depth));
    if (child) {
        return recursive_insert(*child, child, key, key_len, value, depth + 1, old);
    }

    // No child, node goes within us
    woart_leaf *l = make_leaf(key, key_len, value, true);

    add_child(n, ref, get_index(key, depth), SET_LEAF(l));

    return NULL;
}

/**
 * Inserts a new value into the woart tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void *woart_put(woart_tree *t, const unsigned long key, int key_len, void *value, int value_len) {
    int old_val = 0;
    void *value_allocated = concurrency_fast_alloc(value_len);
    woart_memory_usage += value_len;
    memcpy(value_allocated, value, value_len);
    flush_buffer(value_allocated, value_len, true);
    void *old = recursive_insert(t->root, &t->root, key, key_len, value_allocated, 0, &old_val);
    if (!old_val) t->size++;
    return old;
}

void woart_all_subtree_kv(woart_node *n, vector<woart_key_value> &res) {
    if (n == NULL)
        return;
    woart_node *tmp = n;
    woart_node **child;
    if (IS_LEAF(tmp)) {
        tmp = (woart_node *) LEAF_RAW(tmp);
        woart_key_value tmp_kv;
        tmp_kv.key = ((woart_leaf *) tmp)->key;
        tmp_kv.value = *(uint64_t *) (((woart_leaf *) tmp)->value);
        res.push_back(tmp_kv);
    } else {
        // Recursively search
        for (int i = 0; i < 256; ++i) {
            child = find_child(tmp, i);
            woart_node *next = (child) ? *child : NULL;
            woart_all_subtree_kv(next, res);
        }
    }
}

void woart_node_scan(woart_node *n, uint64_t left, uint64_t right, uint64_t depth, vector<woart_key_value> &res,
                     int key_len) {
    //depth first search
    if (n == NULL)
        return;
    woart_node *tmp = n;
    woart_node **child;
    if (IS_LEAF(tmp)) {
        tmp = (woart_node *) LEAF_RAW(tmp);
        // Check if the expanded path matches
        uint64_t tmp_key = ((woart_leaf *) tmp)->key;
        if (tmp_key >= left && tmp_key <= right) {
            woart_key_value tmp_kv;
            tmp_kv.key = tmp_key;
            tmp_kv.value = *(uint64_t *) (((woart_leaf *) tmp)->value);
            res.push_back(tmp_kv);
            return;
        }
    } else {
        if (tmp->path.pwoartial_len) {
            int max_cmp = min(min(tmp->path.pwoartial_len, WOART_MAX_PREFIX_LEN), WOART_MAX_HEIGHT - depth);
            for (int idx = 0; idx < max_cmp; idx++) {
                if (tmp->path.pwoartial[idx] > get_index(left, depth + idx)) {
                    break;
                } else if (tmp->path.pwoartial[idx] < get_index(left, depth + idx)) {
                    return;
                }
            }
            for (int idx = 0; idx < max_cmp; idx++) {
                if (tmp->path.pwoartial[idx] < get_index(right, depth + idx)) {
                    break;
                } else if (tmp->path.pwoartial[idx] > get_index(left, depth + idx)) {
                    return;
                }
            }
            depth = depth + tmp->path.pwoartial_len;
        }
        // Recursively search
        unsigned char left_index = get_index(left, depth);
        unsigned char right_index = get_index(right, depth);

        if (left_index != right_index) {
            child = find_child(tmp, left_index);
            woart_node *next = (child) ? *child : NULL;
            woart_node_scan(next, left, 0xffffffffffffffff, depth + 1, res);
            child = find_child(tmp, right_index);
            next = (child) ? *child : NULL;
            woart_node_scan(next, 0, right, depth + 1, res);

        } else {
            child = find_child(tmp, left_index);
            woart_node *next = (child) ? *child : NULL;
            woart_node_scan(next, left, right, depth + 1, res);
        }

        for (int i = left_index + 1; i < right_index; ++i) {
            child = find_child(tmp, i);
            woart_node *next = (child) ? *child : NULL;
            woart_all_subtree_kv(next, res);
        }
    }
}

vector<woart_key_value> woart_scan(const woart_tree *t, uint64_t left, uint64_t right, int key_len) {
    vector<woart_key_value> res;
    woart_node_scan(t->root, left, right, 0, res);
    return res;
}

uint64_t woart_memory_profile(woart_node *n) {
    return woart_memory_usage;
//    if (n == NULL) {
//        return 0;
//    }
//    uint64_t res = 0;
//    woart_node *tmp = n;
//    woart_node **child;
//    if (IS_LEAF(tmp)) {
//        res += sizeof(woart_leaf);
//    } else {
//        // Recursively search
//        switch (n->type) {
//            case NODE4: {
//                res += sizeof(woart_node4);
//                for (int i = 0; i < 4; ++i) {
//                    child = find_child(tmp, i);
//                    woart_node *next = (child) ? *child : NULL;
//                    res += woart_memory_profile(next);
//                }
//                break;
//            }
//            case NODE16: {
//                res += sizeof(woart_node16);
//                for (int i = 0; i < 16; ++i) {
//                    child = find_child(tmp, i);
//                    woart_node *next = (child) ? *child : NULL;
//                    res += woart_memory_profile(next);
//                }
//                break;
//            }
//            case NODE48: {
//                res += sizeof(woart_node48);
//                for (int i = 0; i < 48; ++i) {
//                    child = find_child(tmp, i);
//                    woart_node *next = (child) ? *child : NULL;
//                    res += woart_memory_profile(next);
//                }
//                break;
//            }
//            case NODE256: {
//                res += sizeof(woart_node256);
//                for (int i = 0; i < 256; ++i) {
//                    child = find_child(tmp, i);
//                    woart_node *next = (child) ? *child : NULL;
//                    res += woart_memory_profile(next);
//                }
//                break;
//            }
//        }
//    }
//    return res;
}