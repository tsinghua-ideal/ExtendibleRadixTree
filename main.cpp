#include <atomic>
#include <iostream>
#include <unistd.h>
#include <map>
#include <sys/time.h>
#include "rng/rng.h"
#include "extendible_radix_tree/ERT_int.h"

using namespace std;

int testNum = 100000;
ERTInt *ert;

unsigned char **keys;
int *lengths;

uint64_t *keys_int;

#define Time_BODY(condition, name, func)                                                        \
    if(condition) {                                                                             \
        sleep(1);                                                                               \
        timeval start, ends;                                                                    \
        gettimeofday(&start, NULL);                                                             \
        for (int i = 0; i < testNum; ++i) {                                                     \
            func                                                                                \
        }                                                                                       \
        gettimeofday(&ends, NULL);                                                              \
        double timeCost = (ends.tv_sec - start.tv_sec) * 1000000 + ends.tv_usec - start.tv_usec;\
        double throughPut = (double) testNum / timeCost;                                        \
        cout << name << testNum << " kv pais in " << timeCost / 1000000 << " s" << endl;        \
        cout << name << "ThroughPut: " << throughPut << " Mops" << endl;                        \
    }


void keys_init() {
//     init test case
    keys = new unsigned char *[testNum];
    lengths = new int[testNum];
    rng r;
    rng_init(&r, 1, 2);
    keys_int = new uint64_t[testNum];
    for (int i = 0; i < testNum; i++) {
        keys_int[i] = rng_next(&r);
    }
}

void correctness_test() {
    for (int i = 0; i < testNum; i++) {
        ert->Insert(keys_int[i], i);
    }

    for (int i = 0; i < testNum; i++) {
        uint64_t res = ert->Search(keys_int[i]);
        if (res != i)
            cout << i << ", " << keys_int[i] << ", " << res << endl;
    }
}

void speed_test() {
    Time_BODY(true, "ERT insert: ", { ert->Insert(keys_int[i], i); })
    Time_BODY(true, "ERT search: ", { ert->Search(keys_int[i]); })
}

int main(int argc, char *argv[]) {
    init_fast_allocator(true);
    testNum = 10000000;

    keys_init();

    // init a new ExtendibleRadixTree
    ert = NewExtendibleRadixTreeInt();

    correctness_test();
    speed_test();

    fast_free();
    return 0;
}
