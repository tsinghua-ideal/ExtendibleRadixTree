#include <atomic>
#include <iostream>
#include <unistd.h>
#include <map>
#include <sys/time.h>
#include "rng/rng.h"
#include "extendible_radix_tree/ERT_int.h"

using namespace std;

int testNum = 100000;
int distribution = 0;
string filePath;
bool onPM = false;

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
    cout << "Start Preparing dataset: " << testNum << " keys" << endl;
    keys = new unsigned char *[testNum];
    lengths = new int[testNum];
    rng r;
    rng_init(&r, 1, 2);
    keys_int = new uint64_t[testNum];
    for (int i = 0; i < testNum; i++) {
        switch(distribution) {
            case 0:
                keys_int[i] = rng_next(&r) % testNum; break;
            case 1:
                keys_int[i] = rng_next(&r); break;
            default:
                keys_int[i] = rng_next(&r); break;
        }
    }
    cout << "Finish dataset preparing." << endl;
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
    sscanf(argv[1], "%d", &testNum);
    sscanf(argv[2], "%d", &distribution);
    if (argc == 4) {
        onPM = true;
        filePath = argv[3];
    }

    //initialize allocator
    init_fast_allocator(true, onPM, filePath);

    // prepare data set
    keys_init();

    // init a new ExtendibleRadixTree
    ert = NewExtendibleRadixTreeInt();

    // evaluate
    speed_test();

    // free allocated DRAM/PM
    fast_free();
    return 0;
}
