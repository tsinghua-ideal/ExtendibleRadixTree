#include <iostream>
#include <stdio.h>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <atomic>
#include <thread>
#include <sys/mman.h>
#include <cstdlib>

#ifndef NVMKV_FASTALLOC_H
#define NVMKV_FASTALLOC_H

#define ALLOC_SIZE ((size_t)4<<30)
#define CONCURRENCY_ALLOC_SIZE ((size_t)4<<28)
#define CACHELINESIZE (64)

#define MAP_SYNC 0x080000
#define MAP_SHARED_VALIDATE 0x03

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))
#define cas(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

using namespace std;

class fastalloc {

public:

    bool is_used = false;
    char *dram[100];
    char *dram_curr = NULL;
    uint64_t dram_left = 0;
    uint64_t dram_cnt = 0;

    char *nvm[100];
    char *nvm_curr = NULL;
    uint64_t nvm_left = 0;
    uint64_t nvm_cnt = 0;

    fastalloc();

    virtual void init();

    virtual void *alloc(uint64_t size, bool _on_nvm = true);

    virtual void free();
};

class concurrency_fastalloc : public fastalloc {

public:
    void init();
};

void init_fast_allocator(bool isMultiThread);

void *fast_alloc(uint64_t size, bool _on_nvm = true);

void *concurrency_fast_alloc(uint64_t size, bool _on_nvm = true);

void fast_free();

#endif
