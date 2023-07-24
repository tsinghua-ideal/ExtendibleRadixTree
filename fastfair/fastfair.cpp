//
// Created by 王柯 on 5/12/21.
//

#include "fastfair.h"

ff_key_value::ff_key_value() {

}

ff_key_value::ff_key_value(uint64_t _key, uint64_t _value) : key(_key), value(_value) {

}

fastfair *new_fastfair() {
    fastfair *new_ff = static_cast<fastfair *>(concurrency_fast_alloc(sizeof(fastfair)));
    new_ff->init();
    return new_ff;
}

/*
 *  class fastfair
 */
fastfair::fastfair() {
    root = (char *) new page();
    height = 1;
}

void fastfair::init() {
    root = (char *) new page();
    height = 1;
}

void fastfair::setNewRoot(char *new_root) {
    this->root = (char *) new_root;
    clflush((char *) &(this->root), sizeof(char *));
    ++height;
}

char *fastfair::get(uint64_t key) {
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL) {
        p = (page *) p->linear_search(key);
    }

    page *t;
    while ((t = (page *) p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p) {
            break;
        }
    }

    if (!t) {
//        printf("NOT FOUND %lu, t = %x\n", key, t);
        return NULL;
    }

    return (char *) t;
}

// insert the key in the leaf node
void fastfair::put(uint64_t key, char *value, int value_len) { // need to be string
    char *value_allocated = static_cast<char *>(concurrency_fast_alloc(value_len));
    memcpy(value_allocated, value, value_len);
    clflush(value_allocated, value_len);
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL) {
        p = (page *) p->linear_search(key);
    }

    if (!p->store(this, NULL, key, value_allocated, true)) { // store
        put(key, value_allocated);
    }
}

// store the key into the node at the given level
void fastfair::fastfair_insert_internal(char *left, uint64_t key, char *value,
                                        uint32_t level) {
    if (level > ((page *) root)->hdr.level)
        return;

    page *p = (page *) this->root;

    while (p->hdr.level > level)
        p = (page *) p->linear_search(key);

    if (!p->store(this, NULL, key, value, true)) {
        fastfair_insert_internal(left, key, value, level);
    }
}

void fastfair::fastfair_delete(uint64_t key) {
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL) {
        p = (page *) p->linear_search(key);
    }

    page *t;
    while ((t = (page *) p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p)
            break;
    }

    if (p) {
        if (!p->remove(this, key)) {
            fastfair_delete(key);
        }
    } else {
        printf("not found the key to delete %lu\n", key);
    }
}

void fastfair::fastfair_delete_internal(uint64_t key, char *ptr, uint32_t level,
                                        uint64_t *deleted_key,
                                        bool *is_leftmost_node, page **left_sibling) {
    if (level > ((page *) this->root)->hdr.level)
        return;

    page *p = (page *) this->root;

    while (p->hdr.level > level) {
        p = (page *) p->linear_search(key);
    }

    if ((char *) p->hdr.leftmost_ptr == ptr) {
        *is_leftmost_node = true;
        return;
    }

    *is_leftmost_node = false;

    for (int i = 0; p->records[i].ptr != NULL; ++i) {
        if (p->records[i].ptr == ptr) {
            if (i == 0) {
                if ((char *) p->hdr.leftmost_ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = p->hdr.leftmost_ptr;
                    p->remove(this, *deleted_key, false, false);
                    break;
                }
            } else {
                if (p->records[i - 1].ptr != p->records[i].ptr) {
                    *deleted_key = p->records[i].key;
                    *left_sibling = (page *) p->records[i - 1].ptr;
                    p->remove(this, *deleted_key, false, false);
                    break;
                }
            }
        }
    }
}

void fastfair::_scan(uint64_t min, uint64_t max,
                     vector<ff_key_value> &buf) {
    page *p = (page *) root;

    while (p) {
        if (p->hdr.leftmost_ptr != NULL) {
            // The current page is internal
            p = (page *) p->linear_search(min);
        } else {
            // Found a leaf
            p->linear_search_range(min, max, buf);

            break;
        }
    }
}

// Function to search keys from "min" to "max"
vector<ff_key_value> fastfair::scan(uint64_t min, uint64_t max) {
    vector<ff_key_value> res;

    _scan(min, max, res);
    return res;
}

uint64_t fastfair::memory_profile(page *p){
    uint64_t res = 0;
    if (p == NULL)
        p = (page *) root;

    res += sizeof(page);
    if (p->hdr.leftmost_ptr != NULL) {
        // The current page is internal
        uint8_t previous_switch_counter = p->hdr.switch_counter;
        for (int i = 0; i < cardinality; ++i) {
            if (p->records[i].ptr != NULL) {
                res += memory_profile((page *)p->records[i].ptr);
            }

        }
    }
    return res;
}


void fastfair::printAll() {
    int total_keys = 0;
    page *leftmost = (page *) root;
    printf("root: %x\n", root);
    if (root) {
        do {
            page *sibling = leftmost;
            while (sibling) {
                if (sibling->hdr.level == 0) {
                    total_keys += sibling->hdr.last_index + 1;
                }
                sibling->print();
                sibling = sibling->hdr.sibling_ptr;
            }
            printf("-----------------------------------------\n");
            leftmost = leftmost->hdr.leftmost_ptr;
        } while (leftmost);
    }

    printf("total number of keys: %d\n", total_keys);
}