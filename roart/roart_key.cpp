//
// Created by 王柯 on 5/25/21.
//

#include "roart_key.h"

ROART_KEY::ROART_KEY() {}

ROART_KEY::ROART_KEY(uint64_t key_, size_t key_len_, uint64_t value_) {
    value = value_;
    key_len = key_len_;
    val_len = sizeof(uint64_t);
    key = key_;
    for (int i = 0; i < 8; ++i) {
        fkey[i] = (key >> ((7 - i) * 8)) & 255;
    }
//    fkey = (uint8_t *) &key;
}

void ROART_KEY::Init(char *key_, size_t key_len_, char *value_, size_t val_len_) {
    val_len = val_len_;
    value = (uint64_t) value_;
    key_len = key_len_;
//    fkey = (uint8_t *) key_;
    for (int i = 0; i < 8; ++i) {
        fkey[i] = (uint8_t)key_[i];
    }
}

void ROART_KEY::Init(uint64_t key_, size_t key_len_, uint64_t value_) {
    value = value_;
    key_len = key_len_;
    val_len = sizeof(uint64_t);
    key = key_;
//    fkey = (uint8_t *)&key;
    for (int i = 0; i < 8; ++i) {
        fkey[i] = (key_ >> ((7 - i) * 8)) & 255;
    }
}

ROART_KEY *ROART_KEY::make_leaf(char *key, size_t key_len, uint64_t value) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(ROART_KEY) + key_len);
    ROART_KEY *k = reinterpret_cast<ROART_KEY *>(aligned_alloc);

    k->value = value;
    k->key_len = key_len;
    memcpy(k->fkey, key, key_len);

    return k;
}

ROART_KEY *ROART_KEY::make_leaf(uint64_t key, size_t key_len, uint64_t value) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(ROART_KEY) + key_len);
    ROART_KEY *k = reinterpret_cast<ROART_KEY *>(aligned_alloc);

    k->value = value;
    k->key_len = key_len;
    reinterpret_cast<uint64_t *>(&k->fkey[0])[0] = __builtin_bswap64(key);

    return k;
}

size_t ROART_KEY::getKeyLen() const {
    return key_len;
}

uint16_t ROART_KEY::getFingerPrint() const {
    uint16_t re = 0;
    for (int i = 0; i < key_len; i++) {
        re = re * 131 + this->fkey[i];
    }
    return re;
}