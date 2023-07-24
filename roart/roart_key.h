//
// Created by 王柯 on 5/25/21.
//

#ifndef NVMKV_KEY_H
#define NVMKV_KEY_H

#include <assert.h>
#include <cstring>
#include <memory>
#include "../fastalloc/fastalloc.h"

struct ROART_KEY {
    uint64_t value;
    size_t key_len;
    size_t val_len;
    uint64_t key;
    uint8_t fkey[8];

    ROART_KEY();

    ROART_KEY(uint64_t key_, size_t key_len_, uint64_t value_);

    void Init(uint64_t key_, size_t key_len_, uint64_t value_);

    void Init(char *key_, size_t key_len_, char *value_, size_t val_len_);

    ROART_KEY *make_leaf(char *key, size_t key_len, uint64_t value);

    ROART_KEY *make_leaf(uint64_t key, size_t key_len, uint64_t value);

    size_t getKeyLen() const;

    uint16_t getFingerPrint() const;
} __attribute__((aligned(64)));

#endif //NVMKV_KEY_H
