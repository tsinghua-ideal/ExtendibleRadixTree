//
// Created by Guanqun Yang on 9/19/21.
//

#include "lbtree.h"
#include <stack>

#ifdef LB_PROFILE_TIME
timeval start_time, end_time;
uint64_t _grow = 0, _update = 0, _travelsal = 0, _decompression = 0;
#endif

#ifdef LB_SCAN_PROFILE_TIME
timeval start_time, end_time;
uint64_t _random, _sequential;
#endif


inline static void mfence() {
    asm volatile("mfence":: :"memory");
}

inline void clflush(char *data, size_t len) {
    volatile char *ptr = (char *) ((unsigned long) data & (~(CACHELINESIZE - 1)));
    mfence();
    for (; ptr < data + len; ptr += CACHELINESIZE) {
        asm volatile("clflush %0" : "+m" (*(volatile char *) ptr));
    }
    mfence();
}

static int last_slot_in_line[LEAF_KEY_NUM];


void freeLockStack(stack<bnode*>& st){
    while(!st.empty()){
        auto i = st.top();
        i->unlock();
        st.pop();
    }
}

static void initUseful(void) {
    // line 0
    last_slot_in_line[0] = 2;
    last_slot_in_line[1] = 2;
    last_slot_in_line[2] = 2;

    // line 1
    last_slot_in_line[3] = 6;
    last_slot_in_line[4] = 6;
    last_slot_in_line[5] = 6;
    last_slot_in_line[6] = 6;

    // line 2
    last_slot_in_line[7] = 10;
    last_slot_in_line[8] = 10;
    last_slot_in_line[9] = 10;
    last_slot_in_line[10] = 10;

    // line 3
    last_slot_in_line[11] = 13;
    last_slot_in_line[12] = 13;
    last_slot_in_line[13] = 13;
}


void treeMeta::setFirstLeaf(bleaf *leaf) {
    *first_leaf = leaf;
    clflush((char *) first_leaf, 8);
}

typedef struct BldThArgs {

    key_type start_key; // input
    key_type num_key;   // input

    int top_level;  // output
    int n_nodes[32];  // output
    Pointer8B pfirst[32];  // output

} BldThArgs;

void *lbtree::lookup(key_type key, int *pos) {
    bnode *p;
    bleaf *lp;
    int i, t, m, b;
    key_type r;

    unsigned char key_hash = hashcode1B(key);
    int ret_pos;

    Again1:
    // 1. RTM begin
//    if (_xbegin() != _XBEGIN_STARTED) goto Again1;

    // 2. search nonleaf nodes
    p = tree_meta->tree_root;

    for (i = tree_meta->root_level; i > 0; i--) {

        // prefetch the entire node
        NODE_PREF(p);

        // if the lock bit is set, abort
        if (p->lock()) {
//            _xabort(1);
            goto Again1;
        }

        // binary search to narrow down to at most 8 entries
        b = 1;
        t = p->num();
        while (b + 7 <= t) {
            m = (b + t) >> 1;
            r = key - p->k(m);
            if (r > 0) b = m + 1;
            else if (r < 0) t = m - 1;
            else {
                p = p->ch(m);
                goto inner_done;
            }
        }

        // sequential search (which is slightly faster now)
        for (; b <= t; b++)
            if (key < p->k(b)) break;
        p = p->ch(b - 1);

        inner_done:;
    }

    // 3. search leaf node
    lp = (bleaf *) p;

    // prefetch the entire node
    LEAF_PREF(lp);

    // if the lock bit is set, abort
    if (lp->lock) {
//        _xabort(2);
        goto Again1;
    }

    // SIMD comparison
    // a. set every byte to key_hash in a 16B register
    __m128i key_16B = _mm_set1_epi8((char) key_hash);

    // b. load meta into another 16B register
    __m128i fgpt_16B = _mm_load_si128((const __m128i *) lp);

    // c. compare them
    __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);

    // d. generate a mask
    unsigned int mask = (unsigned int)
            _mm_movemask_epi8(cmp_res);  // 1: same; 0: diff

    // remove the lower 2 bits then AND bitmap
    mask = (mask >> 2) & ((unsigned int) (lp->bitmap));

    // search every matching candidate
    ret_pos = -1;
    while (mask) {
        int jj = bitScan(mask) - 1;  // next candidate

        if (lp->k(jj) == key) { // found
            ret_pos = jj;
            break;
        }

        mask &= ~(0x1 << jj);  // remove this bit
    } // end while

    // 4. RTM commit
//    _xend();

    if(pos!=0)
        *pos = ret_pos;
    return (void *) lp;
}

void lbtree::qsortBleaf(bleaf *p, int start, int end, int pos[]) {
    if (start >= end) return;

    int pos_start = pos[start];
    key_type key = p->k(pos_start);  // pivot
    int l, r;

    l = start;
    r = end;
    while (l < r) {
        while ((l < r) && (p->k(pos[r]) > key)) r--;
        if (l < r) {
            pos[l] = pos[r];
            l++;
        }
        while ((l < r) && (p->k(pos[l]) <= key)) l++;
        if (l < r) {
            pos[r] = pos[l];
            r--;
        }
    }
    pos[l] = pos_start;
    qsortBleaf(p, start, l - 1, pos);
    qsortBleaf(p, l + 1, end, pos);
}

void lbtree::insert(key_type key, void *_ptr) {
#ifdef LB_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
    void *ptr = (void *)(concurrency_fast_alloc(sizeof(uint64_t)));
    memory_usage += sizeof(uint64_t);
    *(uint64_t *)ptr = *(uint64_t *)_ptr;
    Pointer8B parray[32];  // 0 .. root_level will be used
    short ppos[32];    // 1 .. root_level will be used
    bool isfull[32];  // 0 .. root_level will be used

    unsigned char key_hash = hashcode1B(key);
    volatile long long sum;
    stack<bnode*>lockStack;

    /* Part 1. get the positions to insert the key */
    {
        bnode *p;
        bleaf *lp;
        int i, t, m, b;
        key_type r;

        Again2:

        // 2. search nonleaf nodes
        p = tree_meta->tree_root;

        for (i = tree_meta->root_level; i > 0; i--) {

            // prefetch the entire node
            NODE_PREF(p);

            // if the lock bit is set, abort
            if (p->lock()) {
                goto Again2;
            }

            parray[i] = p;
            isfull[i] = (p->num() == NON_LEAF_KEY_NUM);

            // binary search to narrow down to at most 8 entries
            b = 1;
            t = p->num();
            while (b + 7 <= t) {
                m = (b + t) >> 1;
                r = key - p->k(m);
                if (r > 0) b = m + 1;
                else if (r < 0) t = m - 1;
                else {
                    p = p->ch(m);
                    ppos[i] = m;
                    goto inner_done;
                }
            }

            // sequential search (which is slightly faster now)
            for (; b <= t; b++)
                if (key < p->k(b)) break;
            p = p->ch(b - 1);
            ppos[i] = b - 1;

            inner_done:;
        }

        // 3. search leaf node
        lp = (bleaf *) p;

        // prefetch the entire node
        LEAF_PREF(lp);

        // if the lock bit is set, abort
        if (lp->lock) {
            freeLockStack(lockStack);
            goto Again2;
        }

        parray[0] = lp;

        // SIMD comparison
        // a. set every byte to key_hash in a 16B register
        __m128i key_16B = _mm_set1_epi8((char) key_hash);

        // b. load meta into another 16B register
        __m128i fgpt_16B = _mm_load_si128((const __m128i *) lp);

        // c. compare them
        __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);

        // d. generate a mask
        unsigned int mask = (unsigned int)
                _mm_movemask_epi8(cmp_res);  // 1: same; 0: diff

        // remove the lower 2 bits then AND bitmap
        mask = (mask >> 2) & ((unsigned int) (lp->bitmap));

        // search every matching candidate
        while (mask) {
            int jj = bitScan(mask) - 1;  // next candidate

            if (lp->k(jj) == key) { // found: do nothing, return
                freeLockStack(lockStack);
                return;
            }

            mask &= ~(0x1 << jj);  // remove this bit
        } // end while

        // 4. set lock bits before exiting the RTM transaction
        lp->lock = 1;

        isfull[0] = lp->isFull();
        if (isfull[0]) {
            for (i = 1; i <= tree_meta->root_level; i++) {
                p = parray[i];
                p->lock() = 1;
                if (!isfull[i]) break;
            }
        }


    } // end of Part 1

    /* Part 2. leaf node */
    {
        bleaf *lp = parray[0];
        bleafMeta meta = *((bleafMeta *) lp);

        /* 1. leaf is not full */
#ifdef LB_PROFILE_TIME
        gettimeofday(&end_time, NULL);
        _travelsal += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
        gettimeofday(&start_time, NULL);
#endif
        if (!isfull[0]) {

            meta.v.lock = 0;  // clear lock in temp meta

            // 1.1 get first empty slot
            uint16_t bitmap = meta.v.bitmap;
            int slot = bitScan(~bitmap) - 1;

            // 1.2 set leaf.entry[slot]= (k, v);
            // set fgpt, bitmap in meta
            lp->k(slot) = key;
            lp->ch(slot) = ptr;
            meta.v.fgpt[slot] = key_hash;
            bitmap |= (1 << slot);

            // 1.3 line 0: 0-2; line 1: 3-6; line 2: 7-10; line 3: 11-13
            // in line 0?
            if (slot < 3) {
                // 1.3.1 write word 0
                meta.v.bitmap = bitmap;
                lp->setWord0(&meta);

                // 1.3.2 flush
                clflush((char *) lp, 8);
                freeLockStack(lockStack);
#ifdef LB_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
                gettimeofday(&start_time, NULL);
#endif
                return;
            }

                // 1.4 line 1--3
            else {
                int last_slot = last_slot_in_line[slot];
                int from = 0;
                for (int to = slot + 1; to <= last_slot; to++) {
                    if ((bitmap & (1 << to)) == 0) {
                        // 1.4.1 for each empty slot in the line
                        // copy an entry from line 0
                        lp->ent[to] = lp->ent[from];
                        meta.v.fgpt[to] = meta.v.fgpt[from];
                        bitmap |= (1 << to);
                        bitmap &= ~(1 << from);
                        from++;
                    }
                }

                // 1.4.2 flush the line containing slot
                clflush((char *) &(lp->k(slot)), 8);

                // 1.4.3 change meta and flush line 0
                meta.v.bitmap = bitmap;
                lp->setBothWords(&meta);
                clflush((char *) lp, 8);
                freeLockStack(lockStack);
#ifdef LB_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
                gettimeofday(&start_time, NULL);
#endif
                return;
            }
        } // end of not full
        // 2.1 get sorted positions
        int sorted_pos[LEAF_KEY_NUM];
        for (int i = 0; i < LEAF_KEY_NUM; i++) sorted_pos[i] = i;
        qsortBleaf(lp, 0, LEAF_KEY_NUM - 1, sorted_pos);

        // 2.2 split point is the middle point
        int split = (LEAF_KEY_NUM / 2);  // [0,..split-1] [split,LEAF_KEY_NUM-1]
        key_type split_key = lp->k(sorted_pos[split]);

        // 2.3 create new node
        bleaf *newp = (bleaf *) concurrency_fast_alloc(LEAF_SIZE);
        memory_usage += LEAF_SIZE;

        // 2.4 move entries sorted_pos[split .. LEAF_KEY_NUM-1]
        uint16_t freed_slots = 0;
        for (int i = split; i < LEAF_KEY_NUM; i++) {
            newp->ent[i] = lp->ent[sorted_pos[i]];
            newp->fgpt[i] = lp->fgpt[sorted_pos[i]];

            // add to freed slots bitmap
            freed_slots |= (1 << sorted_pos[i]);
        }
        newp->bitmap = (((1 << (LEAF_KEY_NUM - split)) - 1) << split);
        newp->lock = 0;
        newp->alt = 0;

        // remove freed slots from temp bitmap
        meta.v.bitmap &= ~freed_slots;

        newp->next[0] = lp->next[lp->alt];
        lp->next[1 - lp->alt] = newp;

        // set alt in temp bitmap
        meta.v.alt = 1 - lp->alt;

        // 2.5 key > split_key: insert key into new node
        if (key > split_key) {
            newp->k(split - 1) = key;
            newp->ch(split - 1) = ptr;
            newp->fgpt[split - 1] = key_hash;
            newp->bitmap |= 1 << (split - 1);

            if (tree_meta->root_level > 0) meta.v.lock = 0;  // do not clear lock of root
        }

        // 2.6 clwb newp, clwb lp line[3] and sfence
        clflush((char *) newp, LEAF_LINE_NUM * CACHE_LINE_SIZE);
        clflush((char *) &(lp->next[0]), 8);

        // 2.7 clwb lp and flush: NVM atomic write to switch alt and set bitmap
        lp->setBothWords(&meta);
        clflush((char *) lp, 8);

        // 2.8 key < split_key: insert key into old node
        if (key <= split_key) {

            // note: lock bit is still set
            if (tree_meta->root_level > 0) meta.v.lock = 0;  // do not clear lock of root

            // get first empty slot
            uint16_t bitmap = meta.v.bitmap;
            int slot = bitScan(~bitmap) - 1;

            // set leaf.entry[slot]= (k, v);
            // set fgpt, bitmap in meta
            lp->k(slot) = key;
            lp->ch(slot) = ptr;
            meta.v.fgpt[slot] = key_hash;
            bitmap |= (1 << slot);

            // line 0: 0-2; line 1: 3-6; line 2: 7-10; line 3: 11-13
            // in line 0?
            if (slot < 3) {
                // write word 0
                meta.v.bitmap = bitmap;
                lp->setWord0(&meta);
                // flush
                clflush((char *) lp, 8);
            }
                // line 1--3
            else {
                int last_slot = last_slot_in_line[slot];
                int from = 0;
                for (int to = slot + 1; to <= last_slot; to++) {
                    if ((bitmap & (1 << to)) == 0) {
                        // for each empty slot in the line
                        // copy an entry from line 0
                        lp->ent[to] = lp->ent[from];
                        meta.v.fgpt[to] = meta.v.fgpt[from];
                        bitmap |= (1 << to);
                        bitmap &= ~(1 << from);
                        from++;
                    }
                }

                // flush the line containing slot
                clflush((char *) &(lp->k(slot)), 8);

                // change meta and flush line 0
                meta.v.bitmap = bitmap;
                lp->setBothWords(&meta);
                clflush((char *) lp, 8);
            }
        }

        key = split_key;
        ptr = newp;
        /* (key, ptr) to be inserted in the parent non-leaf */

    } // end of Part 2

    /* Part 3. nonleaf node */
    {
        bnode *p, *newp;
        int n, i, pos, r, lev, total_level;

#define   LEFT_KEY_NUM        ((NON_LEAF_KEY_NUM)/2)
#define   RIGHT_KEY_NUM        ((NON_LEAF_KEY_NUM) - LEFT_KEY_NUM)

        total_level = tree_meta->root_level;
        lev = 1;

        while (lev <= total_level) {

            p = parray[lev];
            n = p->num();
            pos = ppos[lev] + 1;  // the new child is ppos[lev]+1 >= 1

            /* if the non-leaf is not full, simply insert key ptr */

            if (n < NON_LEAF_KEY_NUM) {
                for (i = n; i >= pos; i--) p->ent[i + 1] = p->ent[i];

                p->k(pos) = key;
                p->ch(pos) = ptr;
                p->num() = n + 1;
                mfence();

                // unlock after all changes are globally visible
                p->lock() = 0;
                freeLockStack(lockStack);
#ifdef LB_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _grow += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
                gettimeofday(&start_time, NULL);
#endif
                return;
            }

            /* otherwise allocate a new non-leaf and redistribute the keys */
            newp = (bnode *) concurrency_fast_alloc(NONLEAF_SIZE);
            memory_usage += NONLEAF_SIZE;

            /* if key should be in the left node */
            if (pos <= LEFT_KEY_NUM) {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; r >= 0; r--, i--) {
                    newp->ent[r] = p->ent[i];
                }
                /* newp->key[0] actually is the key to be pushed up !!! */
                for (i = LEFT_KEY_NUM - 1; i >= pos; i--) p->ent[i + 1] = p->ent[i];

                p->k(pos) = key;
                p->ch(pos) = ptr;
            }
                /* if key should be in the right node */
            else {
                for (r = RIGHT_KEY_NUM, i = NON_LEAF_KEY_NUM; i >= pos; i--, r--) {
                    newp->ent[r] = p->ent[i];
                }
                newp->k(r) = key;
                newp->ch(r) = ptr;
                r--;
                for (; r >= 0; r--, i--) {
                    newp->ent[r] = p->ent[i];
                }
            } /* end of else */

            key = newp->k(0);
            ptr = newp;

            p->num() = LEFT_KEY_NUM;
            if (lev < total_level) p->lock() = 0; // do not clear lock bit of root
            newp->num() = RIGHT_KEY_NUM;
            newp->lock() = 0;

            lev++;
        } /* end of while loop */

        /* root was splitted !! add another level */
        newp = (bnode *) concurrency_fast_alloc(NONLEAF_SIZE);
        memory_usage += NONLEAF_SIZE;

        newp->num() = 1;
        newp->lock() = 1;
        newp->ch(0) = tree_meta->tree_root;
        newp->ch(1) = ptr;
        newp->k(1) = key;
        mfence();  // ensure new node is consistent

        void *old_root = tree_meta->tree_root;
        tree_meta->root_level = lev;
        tree_meta->tree_root = newp;
        mfence();   // tree root change is globablly visible
        // old root and new root are both locked

        // unlock old root
        if (total_level > 0) { // previous root is a nonleaf
            ((bnode *) old_root)->lock() = 0;
        } else { // previous root is a leaf
            ((bleaf *) old_root)->lock = 0;
        }

        // unlock new root
        newp->lock() = 0;
        freeLockStack(lockStack);
#ifdef LB_PROFILE_TIME
        gettimeofday(&end_time, NULL);
        _grow += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
        gettimeofday(&start_time, NULL);
#endif
        return;

#undef RIGHT_KEY_NUM
#undef LEFT_KEY_NUM
    }
}

/* ---------------------------------------------------------- *

 deletion

 lazy delete - insertions >= deletions in most cases
 so no need to change the tree structure frequently

 So unless there is no key in a leaf or no child in a non-leaf,
 the leaf and non-leaf won't be deleted.

 * ---------------------------------------------------------- */
void lbtree::del(key_type key) {


    Pointer8B parray[32];  // 0 .. root_level will be used
    short ppos[32];    // 0 .. root_level will be used
    bleaf *leaf_sibp = NULL;  // left sibling of the target leaf

    unsigned char key_hash = hashcode1B(key);
    volatile long long sum;

    /* Part 1. get the positions to insert the key */
    {
        bnode *p;
        bleaf *lp;
        int i, t, m, b;
        key_type r;

        Again3:
        // 1. RTM begin
//        if (_xbegin() != _XBEGIN_STARTED) {
        // random backoff
        // sum= 0;
        // for (int i=(rdtsc() % 1024); i>0; i--) sum += i;
//            goto Again3;
//        }

        // 2. search nonleaf nodes
        p = tree_meta->tree_root;

        for (i = tree_meta->root_level; i > 0; i--) {

            // prefetch the entire node
            NODE_PREF(p);

            // if the lock bit is set, abort
            if (p->lock()) {
//                _xabort(5);
                goto Again3;
            }

            parray[i] = p;

            // binary search to narrow down to at most 8 entries
            b = 1;
            t = p->num();
            while (b + 7 <= t) {
                m = (b + t) >> 1;
                r = key - p->k(m);
                if (r > 0) b = m + 1;
                else if (r < 0) t = m - 1;
                else {
                    p = p->ch(m);
                    ppos[i] = m;
                    goto inner_done;
                }
            }

            // sequential search (which is slightly faster now)
            for (; b <= t; b++)
                if (key < p->k(b)) break;
            p = p->ch(b - 1);
            ppos[i] = b - 1;

            inner_done:;
        }

        // 3. search leaf node
        lp = (bleaf *) p;

        // prefetch the entire node
        LEAF_PREF(lp);

        // if the lock bit is set, abort
        if (lp->lock) {
//            _xabort(6);
            goto Again3;
        }

        parray[0] = lp;

        // SIMD comparison
        // a. set every byte to key_hash in a 16B register
        __m128i key_16B = _mm_set1_epi8((char) key_hash);

        // b. load meta into another 16B register
        __m128i fgpt_16B = _mm_load_si128((const __m128i *) lp);

        // c. compare them
        __m128i cmp_res = _mm_cmpeq_epi8(key_16B, fgpt_16B);

        // d. generate a mask
        unsigned int mask = (unsigned int)
                _mm_movemask_epi8(cmp_res);  // 1: same; 0: diff

        // remove the lower 2 bits then AND bitmap
        mask = (mask >> 2) & ((unsigned int) (lp->bitmap));

        // search every matching candidate
        i = -1;
        while (mask) {
            int jj = bitScan(mask) - 1;  // next candidate

            if (lp->k(jj) == key) { // found: good
                i = jj;
                break;
            }

            mask &= ~(0x1 << jj);  // remove this bit
        } // end while

        if (i < 0) { // not found: do nothing
//            _xend();
            return;
        }

        ppos[0] = i;

        // 4. set lock bits before exiting the RTM transaction
        lp->lock = 1;

        if (lp->num() == 1) {

            // look for its left sibling
            for (i = 1; i <= tree_meta->root_level; i++) {
                if (ppos[i] >= 1) break;
            }

            if (i <= tree_meta->root_level) {
                p = parray[i];
                p = p->ch(ppos[i] - 1);
                i--;

                for (; i >= 1; i--) {
                    p = p->ch(p->num());
                }

                leaf_sibp = (bleaf *) p;
                if (leaf_sibp->lock) {
//                    _xabort(7);
                    goto Again3;
                }

                // lock leaf_sibp
                leaf_sibp->lock = 1;
            }

            // lock affected ancestors
            for (i = 1; i <= tree_meta->root_level; i++) {
                p = (bnode *) parray[i];
                p->lock() = 1;

                if (p->num() >= 1) break;  // at least 2 children, ok to stop
            }
        }

        // 5. RTM commit
//        _xend();

    } // end of Part 1

    /* Part 2. leaf node */
    {
        bleaf *lp = parray[0];

        /* 1. leaf contains more than one key */
        /*    If this leaf node is the root, we cannot delete the root. */
        if ((lp->num() > 1) || (tree_meta->root_level == 0)) {
            bleafMeta meta = *((bleafMeta *) lp);

            meta.v.lock = 0;  // clear lock in temp meta
            meta.v.bitmap &= ~(1 << ppos[0]);  // mark the bitmap to delete the entry
            lp->setWord0(&meta);
            clflush((char *) lp, 8);

            return;

        } // end of more than one key

        /* 2. leaf has only one key: remove the leaf node */

        /* if it has a left sibling */
        if (leaf_sibp != NULL) {
            // remove it from sibling linked list
            leaf_sibp->next[leaf_sibp->alt] = lp->next[lp->alt];
            clflush((char *) &(leaf_sibp->next[0]), 8);

            leaf_sibp->lock = 0;  // lock bit is not protected.
            // It will be reset in recovery
        }

            /* or it is the first child, so let's modify the first_leaf */
        else {
            tree_meta->setFirstLeaf(lp->next[lp->alt]);  // the method calls clwb+sfence
        }

    } // end of Part 2

    /* Part 3: non-leaf node */
    {
        bnode *p, *sibp, *parp;
        int n, i, pos, r, lev;

        lev = 1;

        while (1) {
            p = parray[lev];
            n = p->num();
            pos = ppos[lev];

            /* if the node has more than 1 children, simply delete */
            if (n > 0) {
                if (pos == 0) {
                    p->ch(0) = p->ch(1);
                    pos = 1;  // move the rest
                }
                for (i = pos; i < n; i++) p->ent[i] = p->ent[i + 1];
                p->num() = n - 1;
                mfence();
                // all changes are globally visible now

                // root is guaranteed to have 2 children
                if ((p->num() == 0) && (lev >= tree_meta->root_level)) // root
                    break;

                p->lock() = 0;
                return;
            }


            lev++;
        } /* end of while */

        // p==root has 1 child? so delete the root
        tree_meta->root_level = tree_meta->root_level - 1;
        tree_meta->tree_root = p->ch(0); // running transactions will abort
        mfence();

        return;
    }
}

void lbtree::init() {
    this->tree_meta = static_cast<treeMeta *>(concurrency_fast_alloc(sizeof(treeMeta)));
    auto nvm_address = concurrency_fast_alloc(4 * KB);
    tree_meta->init(nvm_address);
    memory_usage += 4 * KB + sizeof(treeMeta);
}

int lbtree::bulkload(int keynum, key_type input, float bfill) {

    BldThArgs bta;
    bta.top_level = bulkloadSubtree(
            input, 0, keynum, bfill, 31,
            bta.pfirst, bta.n_nodes);
    tree_meta->root_level = bta.top_level;
    tree_meta->tree_root = bta.pfirst[tree_meta->root_level];
    tree_meta->setFirstLeaf(bta.pfirst[0]);
    return tree_meta->root_level;
}

int lbtree::bulkloadSubtree(key_type input, int start_key, int num_key, float bfill, int target_level, Pointer8B pfirst[], int n_nodes[]) {
    int ncur[32];
    int top_level;

    // 1. compute leaf and nonleaf number of keys
    int leaf_fill_num = (int) ((float) LEAF_KEY_NUM * bfill);
    leaf_fill_num = max(leaf_fill_num, 1);

    int nonleaf_fill_num = (int) ((float) NON_LEAF_KEY_NUM * bfill);
    nonleaf_fill_num = max(nonleaf_fill_num, 1);


    // 2. compute number of nodes
    n_nodes[0] = ceiling(num_key, leaf_fill_num);
    top_level = 0;

    for (int i = 1; n_nodes[i - 1] > 1 && i <= target_level; i++) {
        n_nodes[i] = ceiling(n_nodes[i - 1], nonleaf_fill_num + 1);
        top_level = i;
    } // end of for each nonleaf level


    // 3. allocate nodes
    pfirst[0] = concurrency_fast_alloc(sizeof(bleaf) * n_nodes[0]);
    memory_usage += sizeof(bleaf) * n_nodes[0];
    for (int i = 1; i <= top_level; i++) {
        pfirst[i] = concurrency_fast_alloc(sizeof(bnode) * n_nodes[i]);
        memory_usage += sizeof(bleaf) * n_nodes[i];
    }

    // 4. populate nodes
    for (int ll = 1; ll <= top_level; ll++) {
        ncur[ll] = 0;
        bnode *np = (bnode *) (pfirst[ll]);
        np->lock() = 0;
        np->num() = -1;
    }

    bleaf *leaf = pfirst[0];
    int nodenum = n_nodes[0];

    bleafMeta leaf_meta;
    leaf_meta.v.bitmap = (((1 << leaf_fill_num) - 1)
            << (LEAF_KEY_NUM - leaf_fill_num));
    leaf_meta.v.lock = 0;
    leaf_meta.v.alt = 0;

    int key_id = start_key;
    for (int i = 0; i < nodenum; i++) {
        bleaf *lp = &(leaf[i]);

        // compute number of keys in this leaf node
        int fillnum = leaf_fill_num; // in most cases
        if (i == nodenum - 1) {
            fillnum = num_key - (nodenum - 1) * leaf_fill_num;
//            assert(fillnum >= 1 && fillnum <= leaf_fill_num);

            leaf_meta.v.bitmap = (((1 << fillnum) - 1)
                    << (LEAF_KEY_NUM - fillnum));
        }

        // lbtree tends to leave the first line empty
        for (int j = LEAF_KEY_NUM - fillnum; j < LEAF_KEY_NUM; j++) {

            // get key from input
            key_type mykey = input;
            key_id++;

            // entry
            lp->k(j) = mykey;
            lp->ch(j) = (void *) mykey;

            // hash
            leaf_meta.v.fgpt[j] = hashcode1B(mykey);

        } // for each key in this leaf node

        // sibling pointer
        lp->next[0] = ((i < nodenum - 1) ? &(leaf[i + 1]) : NULL);
        lp->next[1] = NULL;

        // 2x8B meta
        lp->setBothWords(&leaf_meta);


        // populate nonleaf node
        Pointer8B child = lp;
        key_type left_key = lp->k(LEAF_KEY_NUM - fillnum);

        // append (left_key, child) to level ll node
        // child is the level ll-1 node to be appended.
        // left_key is the left-most key in the subtree of child.
        for (int ll = 1; ll <= top_level; ll++) {
            bnode *np = ((bnode *) (pfirst[ll])) + ncur[ll];

            // if the node has >=1 child
            if (np->num() >= 0) {
                int kk = np->num() + 1;
                np->ch(kk) = child;
                np->k(kk) = left_key;
                np->num() = kk;

                if ((kk == nonleaf_fill_num) && (ncur[ll] < n_nodes[ll] - 1)) {
                    ncur[ll]++;
                    np++;
                    np->lock() = 0;
                    np->num() = -1;
                }
                break;
            }

            // new node
            np->ch(0) = child;
            np->num() = 0;

            child = np;

        }

    }
    return top_level;
}

vector<lbtree::kv> lbtree::rangeQuery(key_type start , key_type end){
    vector<lbtree::kv> res;
    int pos;
    auto startPointer = (bleaf*)lookup(start,&pos);
    auto endPointer = (bleaf*)lookup(end,&pos);
#ifdef LB_SCAN_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
    for(auto i = startPointer; ;){
        for(int j=0;j<LEAF_KEY_NUM;j++){
            if(i->bitmap&(1<<j) && i->ent[j].k>=start && i->ent[j].k<=end){
                kv tmp;
                tmp.k=i->ent[j].k;
                tmp.v = i->ent[j].ch.value;
//                if(tmp.v != 1)
//                tmp.v = *((uint64_t *)i->ent[j].ch.value);
//                if (tmp.v == 1)
//                    cout << start << ", " << end << ", "<< tmp.k << ", " << tmp.v << endl;
                res.push_back(tmp);
            }
        }
        if(i==endPointer){
            break;
        }else{
            i = i->nextSibling();
        }
    }
#ifdef LB_SCAN_PROFILE_TIME
    gettimeofday(&end_time, NULL);
    _sequential += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
    return res;
}

uint64_t lbtree::memory_profile(){
    return memory_usage;
}


lbtree *new_lbtree() {
    lbtree *mytree = static_cast<lbtree *>(concurrency_fast_alloc(sizeof(lbtree)));
    mytree->memory_usage += sizeof(lbtree);
    mytree->init();
    mytree->bulkload(1, 1);
    return mytree;
}

