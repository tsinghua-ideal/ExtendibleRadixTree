#include <atomic>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <map>
#include <sys/time.h>
#include "rng/rng.h"
#include "extendible_radix_tree/ERT_int.h"
#include "fastfair/fastfair.h"
#include "lbtree/lbtree.h"
#include "wort/wort.h"
#include "woart/woart.h"
#include "roart/roart.h"

using namespace std;

int testNum = 100000;
string filePath;
bool onPM = false;
ofstream out1, out2;

ERTInt *ert1, *ert2;
fastfair *ff1, *ff2;
lbtree *lb1, *lb2;
wort_tree *wort1, *wort2;
woart_tree *woart1, *woart2;
ROART *roart1, *roart2;

uint64_t *keys_sparse;
uint64_t *keys_dense;


#define Time_BODY(condition, name, func, out)                                                        \
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
        out << throughPut << ",";                                                               \
    }


void keys_init() {
//     init test case
    cout << "Start Preparing dataset: " << testNum << " keys" << endl;
    rng r;
    rng_init(&r, 1, 2);
    keys_sparse = new uint64_t[testNum];
    keys_dense = new uint64_t[testNum];
    for (int i = 0; i < testNum; i++) {
        keys_sparse[i] = rng_next(&r);
        keys_dense[i] = rng_next(&r) % testNum;
    }
    cout << "Finish dataset preparing." << endl;
}

void data_structure_init() {
    // init a new ExtendibleRadixTree
    ert1 = NewExtendibleRadixTreeInt();
    ert2 = NewExtendibleRadixTreeInt();

    // init a new Fast&Fair
    ff1 = new_fastfair();
    ff2 = new_fastfair();

    // init a Lb+Tree
    lb1 = new_lbtree();
    lb2 = new_lbtree();

    // init a WORT
    wort1 = new_wort_tree();
    wort2 = new_wort_tree();

    // init a WOART
    woart1 = new_woart_tree();
    woart2 = new_woart_tree();

    // init a ROART
    roart1 = new_roart();
    roart2 = new_roart();
}

void speed_test() {
    out1.open("../Result/insert.csv", ios::out);
    out2.open("../Result/query.csv", ios::out);
    out1 << " ,Dense,Sparse," << endl;
    out2 << " ,Dense,Sparse," << endl;
    int value = 1;

    out1 << "FAST&FAIR,";
    out2 << "FAST&FAIR,";
    Time_BODY(true, "FAST&FAIR insert dense keys: ", { ff1->put(keys_dense[i], (char *) &value); }, out1)
    Time_BODY(true, "FAST&FAIR search dense keys: ", { ff1->get(keys_dense[i]); }, out2)
    Time_BODY(true, "FAST&FAIR insert sparse keys: ", { ff2->put(keys_sparse[i], (char *) &value); }, out1)
    Time_BODY(true, "FAST&FAIR search sparse keys: ", { ff2->get(keys_sparse[i]); }, out2)
    out1 << endl;
    out2 << endl;

    out1 << "LB+Trees,";
    out2 << "LB+Trees,";
    Time_BODY(true, "LB+Trees insert dense keys: ", { lb1->insert(keys_dense[i], (char *) &value); }, out1)
    Time_BODY(true, "LB+Trees search dense keys: ", { lb1->lookup(keys_dense[i]); }, out2)
    Time_BODY(true, "LB+Trees insert sparse keys: ", { lb2->insert(keys_sparse[i], (char *) &value); }, out1)
    Time_BODY(true, "LB+Trees search sparse keys: ", { lb2->lookup(keys_sparse[i]); }, out2)
    out1 << endl;
    out2 << endl;

    out1 << "WORT,";
    out2 << "WORT,";
    Time_BODY(true, "WORT insert dense keys: ", { wort_put(wort1, keys_dense[i], 8, (char *) &value); }, out1)
    Time_BODY(true, "WORT search dense keys: ", { wort_get(wort1, (keys_dense[i]), 8); }, out2)
    Time_BODY(true, "WORT insert sparse keys: ", { wort_put(wort2, keys_sparse[i], 8, (char *) &value); }, out1)
    Time_BODY(true, "WORT search sparse keys: ", { wort_get(wort2, (keys_sparse[i]), 8); }, out2)
    out1 << endl;
    out2 << endl;

    out1 << "WOART, ";
    out2 << "WOART, ";
    Time_BODY(true, "WOART insert dense keys: ", { woart_put(woart1, keys_dense[i], 8, (char *) &value); }, out1)
    Time_BODY(true, "WOART search dense keys: ", { woart_get(woart1, (keys_dense[i]), 8); }, out2)
    Time_BODY(true, "WOART insert sparse keys: ", { woart_put(woart2, keys_sparse[i], 8, (char *) &value); }, out1)
    Time_BODY(true, "WOART search sparse keys: ", { woart_get(woart2, (keys_sparse[i]), 8); }, out2)
    out1 << endl;
    out2 << endl;

    out1 << "ROART,";
    out2 << "ROART,";
    Time_BODY(true, "ROART insert dense keys: ", { roart1->put(keys_dense[i], value); }, out1)
    Time_BODY(true, "ROART search dense keys: ", { roart1->get(keys_dense[i]); }, out2)
    Time_BODY(true, "ROART insert sparse keys: ", { roart2->put(keys_sparse[i], value); }, out1)
    Time_BODY(true, "ROART search sparse keys: ", { roart2->get(keys_sparse[i]); }, out2)
    out1 << endl;
    out2 << endl;

    out1 << "ERT,";
    out2 << "ERT,";
    Time_BODY(true, "ERT insert dense keys: ", { ert1->Insert(keys_dense[i], i); }, out1)
    Time_BODY(true, "ERT search dense keys: ", { ert1->Search(keys_dense[i]); }, out2)
    Time_BODY(true, "ERT insert sparse keys: ", { ert2->Insert(keys_sparse[i], i); }, out1)
    Time_BODY(true, "ERT search sparse keys: ", { ert2->Search(keys_sparse[i]); }, out2)
    out1 << endl;
    out2 << endl;
    cout << "Saved result to ./Result" << endl;
}

int main(int argc, char *argv[]) {
    sscanf(argv[1], "%d", &testNum);
    if (argc == 3) {
        onPM = true;
        filePath = argv[2];
    }

    //initialize allocator
    init_fast_allocator(true, onPM, filePath);

    // prepare data set
    keys_init();

    // init data structure
    data_structure_init();

    // evaluate
    speed_test();

    // free allocated DRAM/PM
    fast_free();
    return 0;
}
