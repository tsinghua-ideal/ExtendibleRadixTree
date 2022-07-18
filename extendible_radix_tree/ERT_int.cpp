#include "ERT_int.h"

/*
 *         begin  len
 * key [______|___________|____________]
 */

#define GET_32BITS(pointer, pos) (*((uint32_t *)(pointer+pos)))

#define _32_BITS_OF_BYTES 4


inline void mfence(void) {
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

bool isSame(unsigned char *key1, uint64_t key2, int pos, int length) {
    uint64_t subkey = GET_SUBKEY(key2, pos, length);
    subkey <<= (64 - length);
    for (int i = 0; i < length / SIZE_OF_CHAR; i++) {
        if ((subkey >> 56) != (uint64_t) key1[i]) {
            return false;
        }
        subkey <<= SIZE_OF_CHAR;
    }
    return true;
}

void ERTInt::Insert(uint64_t key, uint64_t value, ERTIntNode *_node, int len) {
    // 0-index of current bytes

    ERTIntNode *currentNode = _node;
    if (_node == NULL) {
        currentNode = root;
    }
    unsigned char headerDepth = currentNode->header.depth;
    uint64_t beforeAddress = (uint64_t) &root;

    while (len < ERT_KEY_LENGTH / SIZE_OF_CHAR) {
        int matchedPrefixLen;

        // init a number larger than HT_NODE_PREFIX_MAX_LEN to represent there is no value
        if (currentNode->header.len > ERT_NODE_PREFIX_MAX_BYTES) {
            int size = ERT_NODE_PREFIX_MAX_BITS / ERT_NODE_LENGTH * ERT_NODE_LENGTH;

            currentNode->header.len =
                    (ERT_KEY_LENGTH - len) <= size ? (ERT_KEY_LENGTH - len) / SIZE_OF_CHAR : size / SIZE_OF_CHAR;
            currentNode->header.assign(key, len);
            len += size / SIZE_OF_CHAR;
            continue;
        } else {
            // compute this prefix
            matchedPrefixLen = currentNode->header.computePrefix(key, len * SIZE_OF_CHAR);
        }
        if (len + matchedPrefixLen == (ERT_KEY_LENGTH / SIZE_OF_CHAR)) {
            ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
            clflush((char *) kv, sizeof(ERTIntKeyValue));
            currentNode->nodePut(matchedPrefixLen, kv);
            return;
        }
        if (matchedPrefixLen == currentNode->header.len) {
            // if prefix is match
            // move the pos
            len += currentNode->header.len;

            // compute subkey
            // uint64_t subkey = GET_16BITS(key,pos);
            uint64_t subkey = GET_SUBKEY(key, len * SIZE_OF_CHAR, ERT_NODE_LENGTH);

            // use the subkey to search in a cceh node
            uint64_t next = 0;

            uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, currentNode->global_depth);
            ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(currentNode, dir_index);
            uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
            ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
            int i;
            bool keyValueFlag = false;
            uint64_t beforeA;
            for (i = 0; i < ERT_BUCKET_SIZE; ++i) {
                if (subkey == REMOVE_NODE_FLAG(tmp_bucket->counter[i].subkey)) {
                    next = tmp_bucket->counter[i].value;
                    keyValueFlag = GET_NODE_FLAG(tmp_bucket->counter[i].subkey);
                    beforeA = (uint64_t) &tmp_bucket->counter[i].value;
                    break;
                }
            }
            len += ERT_NODE_LENGTH / SIZE_OF_CHAR;
            if (len == 8) {
                if (next == 0) {
                    currentNode->put(subkey, (uint64_t) value, tmp_seg, tmp_bucket, dir_index, seg_index,
                                     beforeAddress);
                    return;
                } else {
                    tmp_bucket->counter[i].value = value;
                    clflush((char *) &tmp_bucket->counter[i].value, 8);
                    return;
                }
            } else {
                if (next == 0) {
                    //not exists
                    ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
                    clflush((char *) kv, sizeof(ERTIntKeyValue));
                    currentNode->put(subkey, (uint64_t) kv, tmp_seg, tmp_bucket, dir_index, seg_index, beforeAddress);
                    return;
                } else {
                    if (keyValueFlag) {
                        // next is key value pair, which means collides
                        uint64_t prekey = ((ERTIntKeyValue *) next)->key;
                        uint64_t prevalue = ((ERTIntKeyValue *) next)->value;
                        if (unlikely(key == prekey)) {
                            //same key, update the value
                            ((ERTIntKeyValue *) next)->value = value;
                            clflush((char *) &(((ERTIntKeyValue *) next)->value), 8);
                            return;
                        } else {
                            //not same key: needs to create a new node
                            ERTIntNode *newNode = NewERTIntNode(ERT_NODE_LENGTH, headerDepth + 1);

                            // put pre kv
                            Insert(prekey, prevalue, newNode, len);

                            // put new kv
                            Insert(key, value, newNode, len);

                            clflush((char *) newNode, sizeof(ERTIntNode));

                            tmp_bucket->counter[i].subkey = REMOVE_NODE_FLAG(tmp_bucket->counter[i].subkey);
                            tmp_bucket->counter[i].value = (uint64_t) newNode;
                            clflush((char *) &tmp_bucket->counter[i].value, 8);
                            return;
                        }
                    } else {
                        // next is next extendible hash
                        currentNode = (ERTIntNode *) next;
                        beforeAddress = beforeA;
                        headerDepth = currentNode->header.depth;
                    }
                }
            }
        } else {
            // if prefix is not match (shorter)
            // split a new tree node and insert

            // build new tree node
            ERTIntNode *newNode = NewERTIntNode(ERT_NODE_LENGTH, headerDepth);
            newNode->header.init(&currentNode->header, matchedPrefixLen, currentNode->header.depth);

            for (int j = currentNode->header.len - matchedPrefixLen; j <= currentNode->header.len; j++) {
                newNode->treeNodeValues[j - currentNode->header.len +
                                        matchedPrefixLen] = currentNode->treeNodeValues[j];
            }

            uint64_t subkey = GET_SUBKEY(key, (len + matchedPrefixLen) * SIZE_OF_CHAR, ERT_NODE_LENGTH);

            ERTIntKeyValue *kv = NewERTIntKeyValue(key, value);
            clflush((char *) kv, sizeof(ERTIntKeyValue));

            newNode->put(subkey, (uint64_t) kv, (uint64_t) &newNode);
            // newNode->put(GET_16BITS(currentNode->header.array,matchedPrefixLen),(uint64_t)currentNode);
            newNode->put(GET_32BITS(currentNode->header.array, matchedPrefixLen), (uint64_t) currentNode,
                         (uint64_t) &newNode);

            // modify currentNode
            currentNode->header.depth -= matchedPrefixLen * SIZE_OF_CHAR / ERT_NODE_LENGTH;
            currentNode->header.len -= matchedPrefixLen;
            for (int i = 0; i < ERT_NODE_PREFIX_MAX_BYTES - matchedPrefixLen; i++) {
                currentNode->header.array[i] = currentNode->header.array[i + matchedPrefixLen];
            }
            clflush((char *) &(currentNode->header), 8);
            clflush((char *) newNode, sizeof(ERTIntNode));

            // modify the successor
            *(ERTIntNode **) beforeAddress = newNode;
            return;
        }
    }
}

uint64_t ERTInt::Search(uint64_t key) {
    auto currentNode = root;
    if (currentNode == NULL) {
        return 0;
    }
    int pos = 0;
    while (pos < ERT_KEY_LENGTH / SIZE_OF_CHAR) {
        if (currentNode->header.len) {
            if (ERT_KEY_LENGTH / SIZE_OF_CHAR - pos <= currentNode->header.len) {
                if (currentNode->treeNodeValues[currentNode->header.len - ERT_KEY_LENGTH + pos].key == key) {
                    return (uint64_t) currentNode->treeNodeValues[currentNode->header.len - ERT_KEY_LENGTH + pos].value;
                } else {
                    return 0;
                }
            }
            if (!isSame((unsigned char *) currentNode->header.array, key, pos * SIZE_OF_CHAR,
                        currentNode->header.len * SIZE_OF_CHAR)) {
                return 0;
            }
            pos += currentNode->header.len;
        }
        // uint64_t subkey = GET_16BITS(key,pos);
        uint64_t subkey = GET_SUBKEY(key, pos * SIZE_OF_CHAR, ERT_NODE_LENGTH);
        bool keyValueFlag = false;
        auto next = currentNode->get(subkey, keyValueFlag);
        // pos+=_16_BITS_OF_BYTES;
        pos += _32_BITS_OF_BYTES;
        if (next == 0) {
            return 0;
        }
        if (keyValueFlag) {
            // is value
            if (pos == 8) {
                return next;
            } else {
                if (key == ((ERTIntKeyValue *) next)->key) {
                    return ((ERTIntKeyValue *) next)->value;
                } else {
                    return 0;
                }
            }
        } else {
            currentNode = (ERTIntNode *) next;
        }
    }
    return 0;
}

void ERTInt::scan(uint64_t left, uint64_t right) {
    vector<ERTIntKeyValue> res;
    nodeScan(root, left, right, res, 0);
//    cout << res.size() << endl;
}


void ERTInt::nodeScan(ERTIntNode *tmp, uint64_t left, uint64_t right, vector<ERTIntKeyValue> &res, int pos,
                      uint64_t prefix) {
    if (unlikely(tmp == NULL)) {
        tmp = root;
    }
    uint64_t leftPos = UINT64_MAX, rightPos = UINT64_MAX;

    for (int i = 0; i < tmp->header.len; i++) {
        uint64_t subkey = GET_SUBKEY(left, pos + i, 8);
        if (subkey == (uint64_t) tmp->header.array[i]) {
            continue;
        } else {
            if (subkey > (uint64_t) tmp->header.array[i]) {
                return;
            } else {
                leftPos = 0;
                break;
            }
        }
    }

    for (int i = 0; i < tmp->header.len; i++) {
        uint64_t subkey = GET_SUBKEY(right, pos + i, 8);
        if (subkey == (uint64_t) tmp->header.array[i]) {
            continue;
        } else {
            if (subkey > (uint64_t) tmp->header.array[i]) {
                rightPos = tmp->dir_size - 1;
                break;
            } else {
                return;
            }
        }
    }

    if (tmp->header.len > 0) {
        prefix = (prefix << tmp->header.len * SIZE_OF_CHAR);
        for (int i = 0; i < tmp->header.len; ++i) {
            prefix += tmp->header.array[i] << (tmp->header.len - i);
        }
        pos += tmp->header.len * SIZE_OF_CHAR;
    }
    uint64_t leftSubkey = UINT64_MAX, rightSubkey = UINT64_MAX;
    if (leftPos == UINT64_MAX) {
        leftSubkey = GET_SUBKEY(left, pos, ERT_NODE_LENGTH);
        leftPos = GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH, tmp->global_depth);
    }
    if (rightPos == UINT64_MAX) {
        rightSubkey = GET_SUBKEY(right, pos, ERT_NODE_LENGTH);
        rightPos = GET_SEG_NUM(rightSubkey, ERT_NODE_LENGTH, tmp->global_depth);
    }
    prefix = (prefix << ERT_NODE_LENGTH);
    pos += ERT_NODE_LENGTH;
    if (leftSubkey == rightSubkey) {
        bool keyValueFlag;
        uint64_t dir_index = GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH, tmp->global_depth);
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, dir_index);
        uint64_t seg_index = GET_BUCKET_NUM(leftSubkey, ERT_BUCKET_MASK_LEN);
        ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
        uint64_t value = tmp_bucket->get(leftSubkey, keyValueFlag);
        if (value == 0 || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(leftSubkey, ERT_NODE_LENGTH,
                                                                                       tmp_seg->depth)))) {
            return;
        }
        if (pos == 64) {
            ERTIntKeyValue tmp;
            tmp.key = leftSubkey + prefix;
            tmp.value = value;
            res.push_back(tmp);
        } else {
            if (keyValueFlag) {
                res.push_back(*(ERTIntKeyValue *) value);
            } else {
                nodeScan((ERTIntNode *) value, left, right, res, pos, prefix + leftSubkey);
            }
        }
        return;
    }
    ERTIntSegment *last_seg = NULL;
    for (uint32_t i = leftPos; i <= rightPos; i++) {
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            continue;
        else
            last_seg = tmp_seg;
        //todo if leftsubkey == rightsubkey, there is no need to scan all the segment.
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                if ((value == 0 && curSubkey == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp,
                                                                                                  GET_SEG_NUM(curSubkey,
                                                                                                              ERT_NODE_LENGTH,
                                                                                                              tmp_seg->depth)))) {
                    continue;
                }
                if ((leftSubkey == UINT64_MAX || curSubkey > leftSubkey) &&
                    (rightSubkey == UINT64_MAX || curSubkey < rightSubkey)) {
                    if (pos == 64) {
                        ERTIntKeyValue tmp;
                        tmp.key = curSubkey + prefix;
                        tmp.value = value;
                        res.push_back(tmp);
                    } else {
                        if (keyValueFlag) {
                            res.push_back(*(ERTIntKeyValue *) value);
                        } else {
                            getAllNodes((ERTIntNode *) value, res, prefix + curSubkey);
                        }
                    }
                } else if (curSubkey == leftSubkey || curSubkey == rightSubkey) {
                    if (pos == 64) {
                        ERTIntKeyValue tmp;
                        tmp.key = curSubkey + prefix;
                        tmp.value = value;
                        res.push_back(tmp);
                    } else {
                        if (pos == ERT_KEY_LENGTH || keyValueFlag) {
                            res.push_back(*(ERTIntKeyValue *) value);
                        } else {
                            nodeScan((ERTIntNode *) value, left, right, res, pos, prefix + curSubkey);
                        }
                    }
                }
            }
        }
    }


}

void ERTInt::getAllNodes(ERTIntNode *tmp, vector<ERTIntKeyValue> &res, int pos, uint64_t prefix) {
    if (tmp == NULL) {
        return;
    }
    if (tmp->header.len > 0) {
        prefix = (prefix << tmp->header.len * SIZE_OF_CHAR);
        for (int i = 0; i < tmp->header.len; ++i) {
            prefix += tmp->header.array[i] << (tmp->header.len - i);
        }
        pos += tmp->header.len * SIZE_OF_CHAR;
    }
    prefix = (prefix << ERT_NODE_LENGTH);
    pos += ERT_NODE_LENGTH;
    ERTIntSegment *last_seg = NULL;
    for (int i = 0; i < tmp->dir_size; i++) {
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            continue;
        else
            last_seg = tmp_seg;
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                if (pos == 64) {
                    ERTIntKeyValue tmp;
                    tmp.key = curSubkey + prefix;
                    tmp.value = value;
                    res.push_back(tmp);
                } else {
                    if ((curSubkey == 0 && value == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(
                            curSubkey, ERT_NODE_LENGTH, tmp_seg->depth)))) {
                        continue;
                    }
                    if (keyValueFlag) {
                        res.push_back(*(ERTIntKeyValue *) value);
                    } else {
                        getAllNodes((ERTIntNode *) value, res, pos, prefix + curSubkey);
                    }
                }
            }
        }
    }
}

uint64_t ERTInt::memory_profile(ERTIntNode *tmp, int pos) {
    if (tmp == NULL) {
        tmp = root;
    }
    uint64_t res = tmp->dir_size * 8 + sizeof(ERTIntNode);
    pos += ERT_NODE_LENGTH;
    ERTIntSegment *last_seg = NULL;
    for (int i = 0; i < tmp->dir_size; i++) {
        ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(tmp, i);
        if (tmp_seg == last_seg)
            continue;
        else {
            last_seg = tmp_seg;
            res += ERT_MAX_BUCKET_NUM * sizeof(ERTIntBucket);
        }
        for (auto j = 0; j < ERT_MAX_BUCKET_NUM; j++) {
            for (auto k = 0; k < ERT_BUCKET_SIZE; k++) {
                bool keyValueFlag = GET_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t curSubkey = REMOVE_NODE_FLAG(tmp_seg->bucket[j].counter[k].subkey);
                uint64_t value = tmp_seg->bucket[j].counter[k].value;
                if (pos != 64) {
                    if ((curSubkey == 0 && value == 0) || (tmp_seg != *(ERTIntSegment **) GET_SEG_POS(tmp, GET_SEG_NUM(
                            curSubkey, ERT_NODE_LENGTH, tmp_seg->depth)))) {
                        continue;
                    }
                    if (keyValueFlag) {
                        res += 16;
                    } else {
                        res += memory_profile((ERTIntNode *) value, pos);
                    }
                }
            }
        }
    }
    return res;
}

ERTInt *NewExtendibleRadixTreeInt() {
    ERTInt *_new_hash_tree = static_cast<ERTInt *>(concurrency_fast_alloc(sizeof(ERTInt)));
    _new_hash_tree->init();
    return _new_hash_tree;
}


ERTInt::ERTInt() {
    root = NewERTIntNode(ERT_NODE_LENGTH);
}

ERTInt::ERTInt(int _span, int _init_depth) {
    init_depth = _init_depth;
    root = NewERTIntNode(ERT_NODE_LENGTH);
}

ERTInt::~ERTInt() {
    delete root;
}

void ERTInt::init() {
    root = NewERTIntNode(ERT_NODE_LENGTH);
}