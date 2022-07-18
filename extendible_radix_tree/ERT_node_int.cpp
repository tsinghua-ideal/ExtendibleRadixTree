#include "ERT_node_int.h"

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

ERTIntKeyValue *NewERTIntKeyValue(uint64_t key, uint64_t value) {
    ERTIntKeyValue *_new_key_value = static_cast<ERTIntKeyValue *>(concurrency_fast_alloc(sizeof(ERTIntKeyValue)));
    _new_key_value->key = key;
    _new_key_value->value = value;
    return _new_key_value;
}


uint64_t ERTIntBucket::get(uint64_t key, bool &keyValueFlag) {
    for (int i = 0; i < ERT_BUCKET_SIZE; ++i) {
        if (key == REMOVE_NODE_FLAG(counter[i].subkey)) {
            keyValueFlag = GET_NODE_FLAG(counter[i].subkey);
            return counter[i].value;
        }
    }
    return 0;
}

int ERTIntBucket::findPlace(uint64_t _key, uint64_t _key_len, uint64_t _depth) {
    // full: return -1
    // exists or not full: return index or empty counter
    int res = -1;
    for (int i = 0; i < ERT_BUCKET_SIZE; ++i) {
        uint64_t removedFlagKey = REMOVE_NODE_FLAG(counter[i].subkey);
        if (_key == removedFlagKey) {
            return i;
        } else if ((res == -1) && removedFlagKey == 0 && counter[i].value == 0) {
            res = i;
        } else if ((res == -1) &&
                   (GET_SEG_NUM(_key, _key_len, _depth) !=
                    GET_SEG_NUM(removedFlagKey, _key_len, _depth))) { // todo: wrong logic
            res = i;
        }
    }
    return res;
}

ERTIntSegment::ERTIntSegment() {
    depth = 0;
    bucket = static_cast<ERTIntBucket *>(concurrency_fast_alloc(sizeof(ERTIntBucket) * ERT_MAX_BUCKET_NUM));
}

ERTIntSegment::~ERTIntSegment() {}

void ERTIntSegment::init(uint64_t _depth) {
    depth = _depth;
    bucket = static_cast<ERTIntBucket *>(concurrency_fast_alloc(sizeof(ERTIntBucket) * ERT_MAX_BUCKET_NUM));
}


ERTIntSegment *NewERTIntSegment(uint64_t _depth) {
    ERTIntSegment *_new_ht_segment = static_cast<ERTIntSegment *>(concurrency_fast_alloc(sizeof(ERTIntSegment)));
    _new_ht_segment->init(_depth);
    return _new_ht_segment;
}

void ERTIntHeader::init(ERTIntHeader *oldHeader, unsigned char length, unsigned char depth) {
    assign(oldHeader->array, length);
    this->depth = depth;
    this->len = length;
}

int ERTIntHeader::computePrefix(uint64_t key, int startPos) {
    if (this->len == 0) {
        return 0;
    }
    uint64_t subkey = GET_SUBKEY(key, startPos, (this->len * SIZE_OF_CHAR));
    subkey <<= (64 - this->len * SIZE_OF_CHAR);
    int res = 0;
    for (int i = 0; i < this->len; i++) {
        if ((subkey >> 56) != ((uint64_t) array[i])) {
            break;
        }
        res++;
    }
    return res;
}

void ERTIntHeader::assign(uint64_t key, int startPos) {
    uint64_t subkey = GET_SUBKEY(key, startPos, (this->len * SIZE_OF_CHAR));
    subkey <<= (ERT_NODE_PREFIX_MAX_BITS - this->len * SIZE_OF_CHAR);
    for (int i = ERT_NODE_PREFIX_MAX_BYTES - 1; i >= 0; i--) {
        array[i] = (char) subkey & (((uint64_t) 1 << 8) - 1);
        subkey >>= 8;
    }
}

void ERTIntHeader::assign(unsigned char *key, unsigned char assignedLength) {
    for (int i = 0; i < assignedLength; i++) {
        array[i] = key[i];
    }
}

ERTIntNode::ERTIntNode() {
    global_depth = 0;
    dir_size = pow(2, global_depth);
    for (int i = 0; i < dir_size; ++i) {
        *(ERTIntSegment **) GET_SEG_POS(this, i) = NewERTIntSegment();
    }
}

ERTIntNode::~ERTIntNode() {}

void ERTIntNode::init(unsigned char headerDepth, unsigned char global_depth) {
    this->global_depth = global_depth;
    this->dir_size = pow(2, global_depth);
    header.depth = headerDepth;
    treeNodeValues = static_cast<ERTIntKeyValue *>(concurrency_fast_alloc(
            sizeof(ERTIntKeyValue) * (1 + ERT_NODE_PREFIX_MAX_BITS / ERT_NODE_LENGTH)));
    for (int i = 0; i < this->dir_size; ++i) {
        *(ERTIntSegment **) GET_SEG_POS(this, i) = NewERTIntSegment(global_depth);
    }
}

void ERTIntNode::put(uint64_t subkey, uint64_t value, uint64_t beforeAddress) {
    uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, global_depth);
    ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(this, dir_index);
    uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
    ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
    put(subkey, value, tmp_seg, tmp_bucket, dir_index, seg_index, beforeAddress);
}

void
ERTIntNode::put(uint64_t subkey, uint64_t value, ERTIntSegment *tmp_seg, ERTIntBucket *tmp_bucket, uint64_t dir_index,
                uint64_t seg_index, uint64_t beforeAddress) {
    int bucket_index = tmp_bucket->findPlace(subkey, ERT_NODE_LENGTH, tmp_seg->depth);
    if (bucket_index == -1) {
        //condition: full
        if (likely(tmp_seg->depth < global_depth)) {
            ERTIntSegment *new_seg = NewERTIntSegment(tmp_seg->depth + 1);
            int64_t stride = pow(2, global_depth - tmp_seg->depth);
            int64_t left = dir_index - dir_index % stride;
            int64_t mid = left + stride / 2, right = left + stride;

            //migrate previous data to the new bucket
            for (int i = 0; i < ERT_MAX_BUCKET_NUM; ++i) {
                uint64_t bucket_cnt = 0;
                for (int j = 0; j < ERT_BUCKET_SIZE; ++j) {
                    uint64_t tmp_key = REMOVE_NODE_FLAG(tmp_seg->bucket[i].counter[j].subkey);
                    uint64_t tmp_value = tmp_seg->bucket[i].counter[j].value;
                    dir_index = GET_SEG_NUM(tmp_key, ERT_NODE_LENGTH, global_depth);
                    if (dir_index >= mid) {
                        ERTIntSegment *dst_seg = new_seg;
                        seg_index = i;
                        ERTIntBucket *dst_bucket = &(dst_seg->bucket[seg_index]);
                        dst_bucket->counter[bucket_cnt].value = tmp_value;
                        dst_bucket->counter[bucket_cnt].subkey = tmp_seg->bucket[i].counter[j].subkey;
                        bucket_cnt++;
                    }
                }
            }
            clflush((char *) new_seg, sizeof(ERTIntSegment));

            // set dir[mid, right) to the new bucket
            for (int i = right - 1; i >= mid; --i) {
                *(ERTIntSegment **) GET_SEG_POS(this, i) = new_seg;
            }
            clflush((char *) GET_SEG_POS(this, right - 1), sizeof(ERTIntSegment *) * (right - mid));

            tmp_seg->depth = tmp_seg->depth + 1;
            clflush((char *) &(tmp_seg->depth), sizeof(tmp_seg->depth));
            this->put(subkey, value, beforeAddress);
            return;
        } else {
            //condition: tmp_bucket->depth == global_depth
            ERTIntNode *newNode = static_cast<ERTIntNode *>(concurrency_fast_alloc(
                    sizeof(ERTIntNode) + sizeof(ERTIntNode *) * dir_size * 2));
            newNode->global_depth = global_depth + 1;
            newNode->dir_size = dir_size * 2;
            newNode->header.init(&this->header, this->header.len, this->header.depth);
            //set dir
            for (int i = 0; i < newNode->dir_size; ++i) {
                *(ERTIntSegment **) GET_SEG_POS(newNode, i) = *(ERTIntSegment **) GET_SEG_POS(this, (i / 2));
            }
            clflush((char *) newNode, sizeof(ERTIntNode) + sizeof(ERTIntSegment *) * newNode->dir_size);
            *(ERTIntNode **) beforeAddress = newNode;
            clflush((char *) beforeAddress, sizeof(ERTIntNode *));
            newNode->put(subkey, value, beforeAddress);
            return;
        }
    } else {
        if (unlikely(tmp_bucket->counter[bucket_index].subkey == subkey) && subkey != 0) {
            //key exists
            tmp_bucket->counter[bucket_index].value = value;
            clflush((char *) &(tmp_bucket->counter[bucket_index].value), 8);
        } else {
            // there is a place to insert
            tmp_bucket->counter[bucket_index].value = value;
            mfence();
            tmp_bucket->counter[bucket_index].subkey = PUT_KEY_VALUE_FLAG(subkey);
            // Here we clflush 16bytes rather than two 8 bytes because all counter are set to 0.
            // If crash after key flushed, then the value is 0. When we return the value, we would find that the key is not inserted.
            clflush((char *) &(tmp_bucket->counter[bucket_index].subkey), 16);
        }
    }
    return;
}

void ERTIntNode::nodePut(int pos, ERTIntKeyValue *kv) {
    treeNodeValues[header.len - pos] = *kv;
}

uint64_t ERTIntNode::get(uint64_t subkey, bool &keyValueFlag) {
    uint64_t dir_index = GET_SEG_NUM(subkey, ERT_NODE_LENGTH, global_depth);
    ERTIntSegment *tmp_seg = *(ERTIntSegment **) GET_SEG_POS(this, dir_index);
    uint64_t seg_index = GET_BUCKET_NUM(subkey, ERT_BUCKET_MASK_LEN);
    ERTIntBucket *tmp_bucket = &(tmp_seg->bucket[seg_index]);
    return tmp_bucket->get(subkey, keyValueFlag);
}


ERTIntNode *NewERTIntNode(int _key_len, unsigned char headerDepth, unsigned char globalDepth) {
    ERTIntNode *_new_node = static_cast<ERTIntNode *>(concurrency_fast_alloc(
            sizeof(ERTIntNode) + sizeof(ERTIntSegment *) * pow(2, globalDepth)));
    _new_node->init(headerDepth, globalDepth);
    return _new_node;
}