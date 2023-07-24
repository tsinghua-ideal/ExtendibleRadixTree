//
// Created by 王柯 on 5/25/21.
//

#include "roart.h"

#ifdef ROART_PROFILE_TIME
extern timeval start_time, end_time;
extern uint64_t _grow, _update, _travelsal, _decompression;
#endif

#ifdef ROART_SCAN_PROFILE_TIME
timeval start_time, end_time;
uint64_t _random, _sequential;
#endif

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

ROART::ROART() {
//    std::cout << "[P-ART]\tnew P-ART\n";

    //    Epoch_Mgr * epoch_mgr = new Epoch_Mgr();
#ifdef ARTPMDK
    const char *pool_name = "/mnt/pmem0/matianmao/dlartpmdk.data";
    const char *layout_name = "DLART";
    size_t pool_size = 64LL * 1024 * 1024 * 1024; // 16GB

    if (access(pool_name, 0)) {
        pmem_pool = pmemobj_create(pool_name, layout_name, pool_size, 0666);
        if (pmem_pool == nullptr) {
            std::cout << "[DLART]\tcreate fail\n";
            assert(0);
        }
        std::cout << "[DLART]\tcreate\n";
    } else {
        pmem_pool = pmemobj_open(pool_name, layout_name);
        std::cout << "[DLART]\topen\n";
    }
    std::cout << "[DLART]\topen pmem pool successfully\n";

    root = new (allocate_size(sizeof(N256))) N256(0, {});
    flush_data((void *)root, sizeof(N256));

#else

    // first open
    root = new(concurrency_fast_alloc(sizeof(N256))) N256(0, {});
    roart_memory_usage += sizeof(N256);
    clflush((char *) root, sizeof(N256));
    //        N::clflush((char *)root, sizeof(N256), true, true);
//    std::cout << "[P-ART]\tfirst create a P-ART\n";


#endif
}

ROART::~ROART() {
    // TODO: reclaim the memory of PM
    //    N::deleteChildren(root);
    //    N::deleteNode(root);
    std::cout << "[P-ART]\tshut down, free the tree\n";
}

#ifdef LEAF_ARRAY

uint64_t ROART::get(uint64_t key) {
    // enter a new epoch
    ROART_KEY *k = new ROART_KEY(key, sizeof(uint64_t), 0);
    bool need_restart;
    int restart_cnt = 0;
    restart:
    need_restart = false;
    N *node = root;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {
#ifdef INSTANT_RESTART
        node->check_generation();
#endif

#ifdef CHECK_COUNT
        int pre = level;
#endif
        switch (checkPrefix(node, k, level)) { // increases level
            case CheckPrefixResult::NoMatch:
                return 0;
            case CheckPrefixResult::OptimisticMatch:
                optimisticPrefixMatch = true;
                // fallthrough
            case CheckPrefixResult::Match: {
                if (k->getKeyLen() <= level) {
                    return 0;
                }
                node = N::getChild(k->fkey[level], node);

#ifdef CHECK_COUNT
                checkcount += std::min(4, (int)level - pre);
#endif

                if (node == nullptr) {
                    return 0;
                }

                if (N::isLeafArray(node)) {

                    auto la = N::getLeafArray(node);
                    //                auto v = la->getVersion();
                    auto ret = la->lookup(k);
                    //                if (la->isObsolete(v) ||
                    //                !la->readVersionOrRestart(v)) {
                    //                    printf("read restart\n");
                    //                    goto restart;
                    //                }
                    if (ret == nullptr && restart_cnt < 0) {
                        restart_cnt++;
                        goto restart;
                    }
                    return *((uint64_t *) ret->value);
                }
            }
        }
        level++;
    }
}

#else
uint64_t ROART::get(uint64_t key) {
    // enter a new epoch
    uint8_t fkey[8];
    for (int i = 0; i < 8; ++i) {
        fkey[i] = (key >> ((7 - i) * 8)) & 255;
    }
    uint64_t key_len = 8;

//    ROART_KEY *k = new ROART_KEY(key, sizeof(uint64_t), 0);
    N *node = root;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {
#ifdef INSTANT_RESTART
        node->check_generation();
#endif

#ifdef CHECK_COUNT
        int pre = level;
#endif
//        switch (checkPrefix(node, k, level)) { // increases level
        switch (mycheckPrefix(node, key_len, fkey, level)) { // increases level
            case CheckPrefixResult::NoMatch:
                return 0;
            case CheckPrefixResult::OptimisticMatch:
                optimisticPrefixMatch = true;
                // fallthrough
            case CheckPrefixResult::Match: {
                if (key_len <= level) {
                    return 0;
                }
                node = N::getChild(fkey[level], node);

#ifdef CHECK_COUNT
                checkcount += std::min(4, (int)level - pre);
#endif

                if (node == nullptr) {
                    return 0;
                }
                if (N::isLeaf(node)) {
                    ROART_Leaf *ret = N::getLeaf(node);
                    if (level < key_len - 1 || optimisticPrefixMatch) {
#ifdef CHECK_COUNT
                        checkcount += k->getKeyLen();
#endif
//                        if (ret->checkKey(k)) {
                        if (ret->mycheckKey(key_len, fkey)) {
                                return *((uint64_t *) ret->value);
                        } else {
                            return 0;
                        }
                    } else {
                        return *((uint64_t *) ret->value);
                    }
                }
            }
        }
        level++;
    }
}
#endif

typename ROART::OperationResults ROART::update(const ROART_KEY *k) const {
    restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    uint8_t nodeKey = 0;
    uint32_t level = 0;
    // bool optimisticPrefixMatch = false;

    while (true) {
        node = nextNode;
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        auto v = node->getVersion(); // check version

        switch (checkPrefix(node, k, level)) { // increases level
            case CheckPrefixResult::NoMatch:
                if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                    goto restart;
                }
                return OperationResults::NotFound;
            case CheckPrefixResult::OptimisticMatch:
                // fallthrough
            case CheckPrefixResult::Match: {
                // if (level >= k->getKeyLen()) {
                //     // key is too short
                //     // but it next fkey is 0
                //     return OperationResults::NotFound;
                // }
                nodeKey = k->fkey[level];

                nextNode = N::getChild(nodeKey, node);

                if (nextNode == nullptr) {
                    if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                        //                        std::cout<<"retry\n";
                        goto restart;
                    }
                    return OperationResults::NotFound;
                }
#ifdef LEAF_ARRAY
                if (N::isLeafArray(nextNode)) {
                    node->lockVersionOrRestart(v, needRestart);
                    if (needRestart) {
                        //                        std::cout<<"retry\n";
                        goto restart;
                    }

                    auto *leaf_array = N::getLeafArray(nextNode);
                    auto leaf = allocLeaf(k);
                    auto result = leaf_array->update(k, leaf);
                    node->writeUnlock();
                    if (!result) {
                        return OperationResults::NotFound;
                    } else {
                        return OperationResults::Success;
                    }
                }
#else
                if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }

                ROART_Leaf *leaf = N::getLeaf(nextNode);
                if (!leaf->checkKey(k)) {
                    node->writeUnlock();
                    return OperationResults::NotFound;
                }
                //
                ROART_Leaf *newleaf = allocLeaf(k);
                //
                N::change(node, nodeKey, N::setLeaf(newleaf));
                node->writeUnlock();
                return OperationResults::Success;
            }
#endif
                level++;
            }
        }
    }
}

#ifdef LEAF_ARRAY

bool ROART::lookupRange(const ROART_KEY *start, const ROART_KEY *end, ROART_KEY *continueKey,
                        ROART_Leaf *result[], std::size_t resultSize,
                        std::size_t &resultsFound) const {
    if (!N::key_key_lt(start, end)) {
        resultsFound = 0;
        return false;
    }
    //    for (uint32_t i = 0; i < std::min(start->getKeyLen(),
    //    end->getKeyLen());
    //         ++i) {
    //        if (start->fkey[i] > end->fkey[i]) {
    //            resultsFound = 0;
    //            return false;
    //        } else if (start->fkey[i] < end->fkey[i]) {
    //            break;
    //        }
    //    }
    char scan_value[100];

    ROART_Leaf *toContinue = nullptr;
    bool restart;
    std::function<void(N *, int, bool, bool)> copy =
            [&result, &resultSize, &resultsFound, &toContinue, &copy, &scan_value,
                    &start, &end](N *node, int compare_level, bool compare_start,
                                  bool compare_end) {
                if (N::isLeafArray(node)) {

                    auto la = N::getLeafArray(node);

                    auto leaves = la->getSortedLeaf(start, end, compare_level,
                                                    compare_start, compare_end);

                    for (auto leaf : leaves) {
                        if (resultsFound == resultSize) {
                            toContinue = N::getLeaf(node);
                            return;
                        }

                        result[resultsFound] = leaf;
                        resultsFound++;
                    }
                } else {
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, 0u, 255u, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        N *n = std::get<1>(children[i]);
                        copy(n, node->getLevel() + 1, compare_start, compare_end);
                        if (toContinue != nullptr) {
                            break;
                        }
                    }
                }
            };
    std::function<void(N *, uint32_t)> findStart =
            [&copy, &start, &findStart, &toContinue, &restart,
                    this](N *node, uint32_t level) {
                if (N::isLeafArray(node)) {
                    copy(node, level, true, false);
                    return;
                }

                PCCompareResults prefixResult;
                prefixResult = checkPrefixCompare(node, start, level);
                switch (prefixResult) {
                    case PCCompareResults::Bigger:
                        copy(node, level, false, false);
                        break;
                    case PCCompareResults::Equal: {
                        uint8_t startLevel = (start->getKeyLen() > level)
                                             ? start->fkey[level]
                                             : (uint8_t) 0;
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        N::getChildren(node, startLevel, 255, children, childrenCount);
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, level + 1);
                            } else if (k > startLevel) {
                                copy(n, level + 1, false, false);
                            }
                            if (toContinue != nullptr || restart) {
                                break;
                            }
                        }
                        break;
                    }
                    case PCCompareResults::SkippedLevel:
                        restart = true;
                        break;
                    case PCCompareResults::Smaller:
                        break;
                }
            };
    std::function<void(N *, uint32_t)> findEnd =
            [&copy, &end, &toContinue, &restart, &findEnd, this](N *node,
                                                                 uint32_t level) {
                if (N::isLeafArray(node)) {
                    // there might be some leaves less than end
                    copy(node, level, false, true);
                    return;
                }

                PCCompareResults prefixResult;
                prefixResult = checkPrefixCompare(node, end, level);

                switch (prefixResult) {
                    case PCCompareResults::Smaller:
                        copy(node, level, false, false);
                        break;
                    case PCCompareResults::Equal: {
                        uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level]
                                                                      : (uint8_t) 255;
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        N::getChildren(node, 0, endLevel, children, childrenCount);
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            N *n = std::get<1>(children[i]);
                            if (k == endLevel) {
                                findEnd(n, level + 1);
                            } else if (k < endLevel) {
                                copy(n, level + 1, false, false);
                            }
                            if (toContinue != nullptr || restart) {
                                break;
                            }
                        }
                        break;
                    }
                    case PCCompareResults::Bigger:
                        break;
                    case PCCompareResults::SkippedLevel:
                        restart = true;
                        break;
                }
            };

    restart:
    restart = false;
    resultsFound = 0;

    uint32_t level = 0;
    N *node = nullptr;
    N *nextNode = root;

    while (true) {
        if (!(node = nextNode) || toContinue)
            break;
        if (N::isLeafArray(node)) {
            copy(node, level, true, true);
            break;
        }

        PCEqualsResults prefixResult;
        prefixResult = checkPrefixEquals(node, level, start, end);
        switch (prefixResult) {
            case PCEqualsResults::SkippedLevel:
                goto restart;
            case PCEqualsResults::NoMatch: {
                return false;
            }
            case PCEqualsResults::Contained: {
                copy(node, level + 1, false, false);
                break;
            }
            case PCEqualsResults::BothMatch: {
                uint8_t startLevel =
                        (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t) 0;
                uint8_t endLevel =
                        (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t) 255;
                if (startLevel != endLevel) {
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, startLevel, endLevel, children,
                                   childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);

                        if (k == startLevel) {
                            findStart(n, level + 1);
                        } else if (k > startLevel && k < endLevel) {
                            copy(n, level + 1, false, false);
                        } else if (k == endLevel) {
                            findEnd(n, level + 1);
                        }
                        if (restart) {
                            goto restart;
                        }
                        if (toContinue) {
                            break;
                        }
                    }
                } else {

                    nextNode = N::getChild(startLevel, node);

                    level++;
                    continue;
                }
                break;
            }
        }
        break;
    }

    if (toContinue != nullptr) {
        ROART_KEY *newkey = new ROART_KEY();
#ifdef KEY_INLINE
        newkey->Init((char *)toContinue->GetKey(), toContinue->key_len,
                     toContinue->GetValue(), toContinue->val_len);
#else
        newkey->Init((char *) toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
#endif
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}

#else
bool ROART::lookupRange(const ROART_KEY *start, const ROART_KEY *end, ROART_KEY *continueKey,
                       ROART_Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
    if (!N::key_key_lt(start, end)) {
        resultsFound = 0;
        return false;
    }
    //    for (uint32_t i = 0; i < std::min(start->getKeyLen(),
    //    end->getKeyLen());
    //         ++i) {
    //        if (start->fkey[i] > end->fkey[i]) {
    //            resultsFound = 0;
    //            return false;
    //        } else if (start->fkey[i] < end->fkey[i]) {
    //            break;
    //        }
    //    }
    char scan_value[100];
    // enter a new epoch

    ROART_Leaf *toContinue = nullptr;
    bool restart;
    std::function<void(N *)> copy = [&result, &resultSize, &resultsFound,
                                     &toContinue, &copy, &scan_value,
                                     start](N *node) {
        if (N::isLeaf(node)) {
            if (resultsFound == resultSize) {
                toContinue = N::getLeaf(node);
                return;
            }
            ROART_Leaf *leaf = N::getLeaf(node);
            result[resultsFound] = N::getLeaf(node);
            resultsFound++;
        } else {
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            N::getChildren(node, 0u, 255u, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
                N *n = std::get<1>(children[i]);
                copy(n);
                if (toContinue != nullptr) {
                    break;
                }
            }
        }
    };
    std::function<void(N *, uint32_t)> findStart =
        [&copy, &start, &findStart, &toContinue, &restart,
         this](N *node, uint32_t level) {
            if (N::isLeaf(node)) {
                // correct the bug
                if (N::leaf_key_lt(N::getLeaf(node), start, level) == false) {
                    copy(node);
                }
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level);
            switch (prefixResult) {
            case PCCompareResults::Bigger:
                copy(node);
                break;
            case PCCompareResults::Equal: {
                uint8_t startLevel = (start->getKeyLen() > level)
                                         ? start->fkey[level]
                                         : (uint8_t)0;
//                uint8_t startLevel = (start->key >> ((start->key_len - level - 1) * 8)) & 255;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, 255, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel) {
                        copy(n);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            case PCCompareResults::Smaller:
                break;
            }
        };
    std::function<void(N *, uint32_t)> findEnd =
        [&copy, &end, &toContinue, &restart, &findEnd, this](N *node,
                                                             uint32_t level) {
            if (N::isLeaf(node)) {
                if (N::leaf_key_lt(N::getLeaf(node), end, level)) {
                    copy(node);
                }
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level);

            switch (prefixResult) {
            case PCCompareResults::Smaller:
                copy(node);
                break;
            case PCCompareResults::Equal: {
                uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level]
                                                              : (uint8_t)255;
//                uint8_t endLevel = (end->key >> ((end->key_len - level - 1) * 8)) & 255;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0, endLevel, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == endLevel) {
                        findEnd(n, level + 1);
                    } else if (k < endLevel) {
                        copy(n);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::Bigger:
                break;
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            }
        };

restart:
    restart = false;
    resultsFound = 0;

    uint32_t level = 0;
    N *node = nullptr;
    N *nextNode = root;

    while (true) {
        if (!(node = nextNode) || toContinue)
            break;
        PCEqualsResults prefixResult;
        prefixResult = checkPrefixEquals(node, level, start, end);
        switch (prefixResult) {
        case PCEqualsResults::SkippedLevel:
            goto restart;
        case PCEqualsResults::NoMatch: {
            return false;
        }
        case PCEqualsResults::Contained: {
            copy(node);
            break;
        }
        case PCEqualsResults::BothMatch: {
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t)0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t)255;
//            cout << startLevel - 0 << endl << endLevel - 0 << endl;
//            uint8_t startLevel = (start->key >> ((start->key_len - level - 1) * 8)) & 255;
//            uint8_t endLevel = (end->key >> ((end->key_len - level - 1) * 8)) & 255;
            if (startLevel != endLevel) {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, endLevel, children,
                               childrenCount);
#ifdef ROART_SCAN_PROFILE_TIME
                gettimeofday(&start_time, NULL);
#endif
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel && k < endLevel) {
                        copy(n);
                    } else if (k == endLevel) {
                        findEnd(n, level + 1);
                    }
                    if (restart) {
                        goto restart;
                    }
                    if (toContinue) {
                        break;
                    }
                }
#ifdef ROART_SCAN_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _sequential += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
            } else {

                nextNode = N::getChild(startLevel, node);

                level++;
                continue;
            }
            break;
        }
        }
        break;
    }

    if (toContinue != nullptr) {
        ROART_KEY *newkey = new ROART_KEY();
#ifdef KEY_INLINE
        newkey->Init((char *)toContinue->GetKey(), toContinue->key_len,
                     toContinue->GetValue(), toContinue->val_len);
#else
        newkey->Init((char *)toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
#endif
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}
#endif

vector<ROART_KEY> ROART::scan(uint64_t min, uint64_t max, uint64_t size) {
    vector<ROART_KEY> res;
    ROART_KEY *start, *end, *continue_key;
    size_t res_cnt = 0;
    size_t res_len = size ? size : max - min;
    start = new ROART_KEY(min, sizeof(uint64_t), 0);
    end = new ROART_KEY(max, sizeof(uint64_t), 0);
    continue_key = NULL;
    ROART_Leaf **result = new ROART_Leaf *[res_len];
    lookupRange(start, end, continue_key, result, res_len, res_cnt);
//    cout << res_cnt << endl;
    return res;
}


typename ROART::OperationResults ROART::put(uint64_t key, uint64_t value) {

//    ROART_KEY *k;
//    k = new ROART_KEY(key, sizeof(uint64_t), value);
//    uint8_t *fkey = (uint8_t *) &key;

    uint8_t fkey[8];
    for (int i = 0; i < 8; ++i) {
        fkey[i] = (key >> ((7 - i) * 8)) & 255;
    }
    unsigned long key_len = 8;
    unsigned long val_len = 8;

    restart:
    bool needRestart = false;
    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        auto v = node->getVersion();

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey;
        Prefix remainingPrefix;
        switch (
//                checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
//                                       remainingPrefix)) { // increases nextLevel
                mycheckPrefixPessimistic(node, fkey, nextLevel, nonMatchingKey,
                                       remainingPrefix)) { // increases nextLevel
            case CheckPrefixPessimisticResult::SkippedLevel:
                goto restart;
            case CheckPrefixPessimisticResult::NoMatch: {

//                assert(nextLevel < k->getKeyLen()); // prevent duplicate key
                assert(nextLevel < key_len); // prevent duplicate key
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart)
                    goto restart;

                // 1) Create new node which will be parent of node, Set common
                // prefix, level to this node
                Prefix prefi = node->getPrefi();
                prefi.prefixCount = nextLevel - level;
#ifdef ROART_PROFILE_TIME
                gettimeofday(&start_time, NULL);
#endif
#ifdef ARTPMDK
                N4 *newNode = new (allocate_size(sizeof(N4))) N4(nextLevel, prefi);
#else
                auto newNode = new(concurrency_fast_alloc(get_node_size(NTypes::N4)))
                        N4(nextLevel, prefi); // not persist
                roart_memory_usage += get_node_size(NTypes::N4);
#endif

                // 2)  add node and (tid, *k) as children

//                auto *newLeaf = allocLeaf(k);
                auto *newLeaf = allocLeaf(key, value, fkey);
#ifdef LEAF_ARRAY
                auto newLeafArray =
                        new(concurrency_fast_alloc(get_node_size(NTypes::LeafArray))) LeafArray();
                newLeafArray->insert(newLeaf, true);
                newNode->insert(fkey[nextLevel], N::setLeafArray(newLeafArray),
                                false);
//                newNode->insert(k->fkey[nextLevel], N::setLeafArray(newLeafArray),
//                                false);
#else
//                newNode->insert(k->fkey[nextLevel], N::setLeaf(newLeaf), false);
                newNode->insert(fkey[nextLevel], N::setLeaf(newLeaf), false);

#endif
                // not persist
                newNode->insert(nonMatchingKey, node, false);
                // persist the new node
                clflush((char *) newNode, sizeof(N4));

                // 3) lockVersionOrRestart, update parentNode to point to the
                // new node, unlock
                parentNode->writeLockOrRestart(needRestart);
                if (needRestart) {
                    node->writeUnlock();
                    goto restart;
                }

                N::change(parentNode, parentKey, newNode);
                parentNode->writeUnlock();

                // 4) update prefix of node, unlock
                node->setPrefix(
                        remainingPrefix.prefix,
                        node->getPrefi().prefixCount - ((nextLevel - level) + 1), true);
                //            std::cout<<"insert success\n";

                node->writeUnlock();
#ifdef ROART_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _decompression += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
                return OperationResults::Success;

            } // end case  NoMatch
            case CheckPrefixPessimisticResult::Match:
                break;
        }
//        assert(nextLevel < k->getKeyLen()); // prevent duplicate key
        assert(nextLevel < key_len); // prevent duplicate key
        // TODO: maybe one string is substring of another, so it fkey[level]
        // will be 0 solve problem of substring

        level = nextLevel;
        nodeKey = fkey[level];
//        nodeKey = k->fkey[level];

        nextNode = N::getChild(nodeKey, node);
        if (nextNode == nullptr) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
//            ROART_Leaf *newLeaf = allocLeaf(k);
#ifdef ROART_PROFILE_TIME
            gettimeofday(&start_time, NULL);
#endif
            ROART_Leaf *newLeaf = allocLeaf(key, value, fkey);
#ifdef ROART_PROFILE_TIME
            gettimeofday(&end_time, NULL);
//            _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
#ifdef LEAF_ARRAY
#ifdef ROART_PROFILE_TIME
            gettimeofday(&start_time, NULL);
#endif
            auto newLeafArray =
                    new(concurrency_fast_alloc(get_node_size(NTypes::LeafArray))) LeafArray();
#ifdef ROART_PROFILE_TIME
            gettimeofday(&end_time, NULL);
            _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
            newLeafArray->insert(newLeaf, true);
            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeafArray(newLeafArray), needRestart);
#else
            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeaf(newLeaf), needRestart);
#endif
            if (needRestart)
                goto restart;
            return OperationResults::Success;
        }
#ifdef LEAF_ARRAY
        if (N::isLeafArray(nextNode)) {
            auto leaf_array = N::getLeafArray(nextNode);
//            if (leaf_array->lookup(k) != nullptr) {
            if (leaf_array->mylookup(key, key_len, fkey) != nullptr) {
                return OperationResults::Existed;
            } else {
                auto lav = leaf_array->getVersion();
                leaf_array->lockVersionOrRestart(lav, needRestart);
                if (needRestart) {
                    goto restart;
                }
                if (leaf_array->isFull()) {
                    leaf_array->splitAndUnlock(node, nodeKey, needRestart);
                    if (needRestart) {
                        goto restart;
                    }
                    nextNode = N::getChild(nodeKey, node);
                    // insert at the next iteration
                } else {
//                    auto leaf = allocLeaf(k);
#ifdef ROART_PROFILE_TIME
                    gettimeofday(&start_time, NULL);
#endif
                    ROART_Leaf *leaf = allocLeaf(key, value, fkey);
                    leaf_array->insert(leaf, true);
                    leaf_array->writeUnlock();
#ifdef ROART_PROFILE_TIME
                    gettimeofday(&end_time, NULL);
                    _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
                    return OperationResults::Success;
                }
            }
        }
#else
        if (N::isLeaf(nextNode)) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
            ROART_Leaf *leaf = N::getLeaf(nextNode);

            level++;
            // assert(level < leaf->getKeyLen());
            // prevent inserting when
            // prefix of leaf exists already
            // but if I want to insert a prefix of this leaf, i also need to
            // insert successfully
            uint32_t prefixLength = 0;
#ifdef KEY_INLINE
            while (level + prefixLength <
                       std::min(k->getKeyLen(), leaf->getKeyLen()) &&
                   leaf->kv[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
#else
//            while (level + prefixLength <
//                       std::min(k->getKeyLen(), leaf->getKeyLen()) &&
//                   leaf->fkey[level + prefixLength] ==
//                       k->fkey[level + prefixLength]) {
//                prefixLength++;
//            }
            while (level + prefixLength <
                   std::min(key_len, leaf->getKeyLen()) &&
                   leaf->fkey[level + prefixLength] ==
                   fkey[level + prefixLength]) {
                prefixLength++;
            }
#endif
            // equal
            if (key_len == leaf->getKeyLen() &&
                level + prefixLength == key_len) {
                // duplicate leaf
#ifdef ROART_PROFILE_TIME
                gettimeofday(&start_time, NULL);
#endif
                memcpy(leaf->value, &(value), val_len);
                clflush(leaf->value, val_len);
                node->writeUnlock();
                //                std::cout<<"ohfinish\n";
#ifdef ROART_PROFILE_TIME
                gettimeofday(&end_time, NULL);
                _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
                return OperationResults::Existed;
            }
            // substring

#ifdef ARTPMDK
            N4 *n4 = new (allocate_size(sizeof(N4)))
                N4(level + prefixLength, &k->fkey[level],
                   prefixLength); // not persist
#else
#ifdef ROART_PROFILE_TIME
            gettimeofday(&start_time, NULL);
#endif
            auto n4 = new (concurrency_fast_alloc(get_node_size(NTypes::N4)))
                    N4(level + prefixLength, &fkey[level],
                       prefixLength); // not persist
//            auto n4 = new (concurrency_fast_alloc(get_node_size(NTypes::N4)))
//                N4(level + prefixLength, &k->fkey[level],
//                   prefixLength); // not persist
//                   prefixLength); // not persist
            roart_memory_usage += get_node_size(NTypes::N4);
#endif
//            ROART_Leaf *newLeaf = allocLeaf(k);
            ROART_Leaf *newLeaf = allocLeaf(key, value, fkey);
//            n4->insert(k->fkey[level + prefixLength], N::setLeaf(newLeaf),
//                       false);
            n4->insert(fkey[level + prefixLength], N::setLeaf(newLeaf),
                       false);
#ifdef KEY_INLINE
            n4->insert(leaf->kv[level + prefixLength], nextNode, false);
#else
            n4->insert(leaf->fkey[level + prefixLength], nextNode, false);
#endif
            clflush((char *)n4, sizeof(N4));

//            N::change(node, k->fkey[level - 1], n4);
            N::change(node, fkey[level - 1], n4);
            node->writeUnlock();
#ifdef ROART_PROFILE_TIME
            gettimeofday(&end_time, NULL);
            _grow += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
            return OperationResults::Success;
        }
#endif
        level++;
    }
    //    std::cout<<"ohfinish\n";
}

typename ROART::OperationResults ROART::remove(const ROART_KEY *k) {
    restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    // bool optimisticPrefixMatch = false;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        auto v = node->getVersion();

        switch (checkPrefix(node, k, level)) { // increases level
            case CheckPrefixResult::NoMatch:
                if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                    goto restart;
                }
                return OperationResults::NotFound;
            case CheckPrefixResult::OptimisticMatch:
                // fallthrough
            case CheckPrefixResult::Match: {
                // if (level >= k->getKeyLen()) {
                //     // key is too short
                //     // but it next fkey is 0
                //     return OperationResults::NotFound;
                // }
                nodeKey = k->fkey[level];

                nextNode = N::getChild(nodeKey, node);

                if (nextNode == nullptr) {
                    if (N::isObsolete(v) ||
                        !node->readVersionOrRestart(v)) { // TODO
                        goto restart;
                    }
                    return OperationResults::NotFound;
                }
#ifdef LEAF_ARRAY
                if (N::isLeafArray(nextNode)) {
                    auto *leaf_array = N::getLeafArray(nextNode);
                    auto lav = leaf_array->getVersion();
                    leaf_array->lockVersionOrRestart(lav, needRestart);
                    if (needRestart) {
                        goto restart;
                    }
                    auto result = leaf_array->remove(k);
                    leaf_array->writeUnlock();
                    if (!result) {
                        return OperationResults::NotFound;
                    } else {
                        return OperationResults::Success;
                    }
                }
#else
                if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart)
                    goto restart;

                ROART_Leaf *leaf = N::getLeaf(nextNode);
                if (!leaf->checkKey(k)) {
                    node->writeUnlock();
                    return OperationResults::NotFound;
                }
                assert(parentNode == nullptr || N::getCount(node) != 1);
                if (N::getCount(node) == 2 && node != root) {
                    // 1. check remaining entries
                    N *secondNodeN;
                    uint8_t secondNodeK;
                    std::tie(secondNodeN, secondNodeK) =
                        N::getSecondChild(node, nodeKey);
                    if (N::isLeaf(secondNodeN)) {
                        parentNode->writeLockOrRestart(needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            goto restart;
                        }

                        // N::remove(node, k[level]); not necessary
                        N::change(parentNode, parentKey, secondNodeN);

                        parentNode->writeUnlock();
                        node->writeUnlockObsolete();

                        // remove the node
                    } else {
                        uint64_t vChild = secondNodeN->getVersion();
                        secondNodeN->lockVersionOrRestart(vChild, needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            goto restart;
                        }
                        parentNode->writeLockOrRestart(needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            secondNodeN->writeUnlock();
                            goto restart;
                        }

                        // N::remove(node, k[level]); not necessary
                        N::change(parentNode, parentKey, secondNodeN);

                        secondNodeN->addPrefixBefore(node, secondNodeK);

                        parentNode->writeUnlock();
                        node->writeUnlockObsolete();

                        // remove the node
                        secondNodeN->writeUnlock();
                    }
                } else {
                    N::removeAndUnlock(node, k->fkey[level], parentNode,
                                       parentKey, needRestart);
                    if (needRestart)
                        goto restart;
                }
                // remove the leaf

                return OperationResults::Success;
            }
#endif
                level++;
            }
        }
    }
}

ROART_Leaf *ROART::allocLeaf(const ROART_KEY *k) const {
#ifdef KEY_INLINE

#ifdef ARTPMDK
    ROART_Leaf *newLeaf =
        new (allocate_size(sizeof(ROART_Leaf) + k->key_len + k->val_len)) ROART_Leaf(k);
    flush_data((void *)newLeaf, sizeof(ROART_Leaf) + k->key_len + k->val_len);
#else

    ROART_Leaf *newLeaf =
        new (alloc_new_node_from_size(sizeof(ROART_Leaf) + k->key_len + k->val_len))
            ROART_Leaf(k);
    flush_data((void *)newLeaf, sizeof(ROART_Leaf) + k->key_len + k->val_len);
#endif
    return newLeaf;
#else
    ROART_Leaf *newLeaf =
            new(concurrency_fast_alloc(get_node_size(NTypes::ROART_Leaf))) ROART_Leaf(k); // not persist
    roart_memory_usage += get_node_size(NTypes::ROART_Leaf);
    clflush((char *) newLeaf, sizeof(ROART_Leaf));
    return newLeaf;
#endif
}

ROART_Leaf *ROART::allocLeaf(uint64_t _key, uint64_t _value, uint8_t *_fkey) const {
#ifdef KEY_INLINE

    #ifdef ARTPMDK
    ROART_Leaf *newLeaf =
        new (allocate_size(sizeof(ROART_Leaf) + k->key_len + k->val_len)) ROART_Leaf(k);
    flush_data((void *)newLeaf, sizeof(ROART_Leaf) + k->key_len + k->val_len);
#else

    ROART_Leaf *newLeaf =
        new (alloc_new_node_from_size(sizeof(ROART_Leaf) + k->key_len + k->val_len))
            ROART_Leaf(k);
    flush_data((void *)newLeaf, sizeof(ROART_Leaf) + k->key_len + k->val_len);
#endif
    return newLeaf;
#else
    ROART_Leaf *newLeaf =
            new(concurrency_fast_alloc(get_node_size(NTypes::ROART_Leaf))) ROART_Leaf(_key, _value, _fkey); // not persist
    roart_memory_usage += get_node_size(NTypes::ROART_Leaf);
    clflush((char *) newLeaf, sizeof(ROART_Leaf));
    return newLeaf;
#endif
}

typename ROART::CheckPrefixResult ROART::checkPrefix(N *n, const ROART_KEY *k,
                                                     uint32_t &level) {
    if (k->getKeyLen() <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != k->fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
    }
    return CheckPrefixResult::Match;
}

typename ROART::CheckPrefixResult ROART::mycheckPrefix(N *n, uint64_t key_len, uint8_t *fkey,
                                                     uint32_t &level) {
    if (key_len <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
    }
    return CheckPrefixResult::Match;
}

typename ROART::CheckPrefixPessimisticResult
ROART::checkPrefixPessimistic(N *n, const ROART_KEY *k, uint32_t &level,
                              uint8_t &nonMatchingKey,
                              Prefix &nonMatchingPrefix) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level != n->getLevel()) {
        // Intermediate or inconsistent state from path compression
        // "splitAndUnlock" or "merge" is detected Inconsistent path compressed
        // prefix should be recovered in here
        bool needRecover = false;
        auto v = n->getVersion();
        n->lockVersionOrRestart(v, needRecover);
        if (!needRecover) {
            // Inconsistent state due to prior system crash is suspected --> Do
            // recovery

            // 1) Picking up arbitrary two leaf nodes and then 2) rebuilding
            // correct compressed prefix
            uint32_t discrimination =
                    (n->getLevel() > level ? n->getLevel() - level
                                           : level - n->getLevel());
            ROART_Leaf *kr = N::getAnyChildTid(n);
            p.prefixCount = discrimination;
            for (uint32_t i = 0;
                 i < std::min(discrimination, maxStoredPrefixLength); i++) {
#ifdef KEY_INLINE
                p.prefix[i] = kr->kv[level + i];
#else
                p.prefix[i] = kr->fkey[level + i];
#endif
            }
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression splitAndUnlock is in progress --> skipping an
        // intermediate compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        ROART_Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                //            if (i == maxStoredPrefixLength) {
                // Optimistic path compression
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                    i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey != k->fkey[level]) {
                nonMatchingKey = curKey;
                if (p.prefixCount > maxStoredPrefixLength) {
                    if (i < maxStoredPrefixLength) {
                        kt = N::getAnyChildTid(n);
                    }
                    for (uint32_t j = 0;
                         j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                      maxStoredPrefixLength);
                         ++j) {
#ifdef KEY_INLINE
                        nonMatchingPrefix.prefix[j] =
                            (uint8_t)kt->kv[level + j + 1];
#else
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
#endif
                    }
                } else {
                    for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                        nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                    }
                }
                return CheckPrefixPessimisticResult::NoMatch;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::Match;
}

typename ROART::CheckPrefixPessimisticResult
ROART::mycheckPrefixPessimistic(N *n, uint8_t *fkey, uint32_t &level,
                              uint8_t &nonMatchingKey,
                              Prefix &nonMatchingPrefix) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level != n->getLevel()) {
        // Intermediate or inconsistent state from path compression
        // "splitAndUnlock" or "merge" is detected Inconsistent path compressed
        // prefix should be recovered in here
        bool needRecover = false;
        auto v = n->getVersion();
        n->lockVersionOrRestart(v, needRecover);
        if (!needRecover) {
            // Inconsistent state due to prior system crash is suspected --> Do
            // recovery

            // 1) Picking up arbitrary two leaf nodes and then 2) rebuilding
            // correct compressed prefix
            uint32_t discrimination =
                    (n->getLevel() > level ? n->getLevel() - level
                                           : level - n->getLevel());
            ROART_Leaf *kr = N::getAnyChildTid(n);
            p.prefixCount = discrimination;
            for (uint32_t i = 0;
                 i < std::min(discrimination, maxStoredPrefixLength); i++) {
#ifdef KEY_INLINE
                p.prefix[i] = kr->kv[level + i];
#else
                p.prefix[i] = kr->fkey[level + i];
#endif
            }
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression splitAndUnlock is in progress --> skipping an
        // intermediate compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        ROART_Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                //            if (i == maxStoredPrefixLength) {
                // Optimistic path compression
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                    i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey != fkey[level]) {
                nonMatchingKey = curKey;
                if (p.prefixCount > maxStoredPrefixLength) {
                    if (i < maxStoredPrefixLength) {
                        kt = N::getAnyChildTid(n);
                    }
                    for (uint32_t j = 0;
                         j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                      maxStoredPrefixLength);
                         ++j) {
#ifdef KEY_INLINE
                        nonMatchingPrefix.prefix[j] =
                            (uint8_t)kt->kv[level + j + 1];
#else
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
#endif
                    }
                } else {
                    for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                        nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                    }
                }
                return CheckPrefixPessimisticResult::NoMatch;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::Match;
}

typename ROART::PCCompareResults
ROART::checkPrefixCompare(const N *n, const ROART_KEY *k, uint32_t &level) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCCompareResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        ROART_Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t kLevel =
                    (k->getKeyLen() > level) ? k->fkey[level] : (uint8_t) 0;

#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                    i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey < kLevel) {
                return PCCompareResults::Smaller;
            } else if (curKey > kLevel) {
                return PCCompareResults::Bigger;
            }
            ++level;
        }
    }
    return PCCompareResults::Equal;
}

typename ROART::PCEqualsResults ROART::checkPrefixEquals(const N *n,
                                                         uint32_t &level,
                                                         const ROART_KEY *start,
                                                         const ROART_KEY *end) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCEqualsResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        ROART_Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t startLevel =
                    (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t) 0;
            uint8_t endLevel =
                    (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t) 0;

#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                    i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey > startLevel && curKey < endLevel) {
                return PCEqualsResults::Contained;
            } else if (curKey < startLevel || curKey > endLevel) {
                return PCEqualsResults::NoMatch;
            }
            ++level;
        }
    }
    return PCEqualsResults::BothMatch;
}

void ROART::graphviz_debug() {
    std::ofstream f("/Users/wangke/CLionProjects/nvmkv/tree-view.dot");

    f << "graph tree\n"
         "{\n"
         "    graph[dpi = 400];\n"
         "    label=\"Tree View\"\n"
         "    node []\n";
    N::graphviz_debug(f, root);
    f << "}";
    f.close();
    //    printf("ok2\n");
}

ROART *new_roart() {
    ROART *_new_roart = new(concurrency_fast_alloc(sizeof(ROART))) ROART();
    roart_memory_usage += sizeof(ROART);
    return _new_roart;
}

uint64_t ROART::memory_profile(N *tmp) {
//    return roart_memory_usage;
    uint64_t res = 0;
    if(tmp == nullptr) {
        tmp = root;
    }
    if (N::isLeaf(tmp)) {
        res += sizeof(ROART_Leaf);
    } else {
        res += get_node_size(tmp->getType());
        std::tuple<uint8_t, N *> children[256];
        uint32_t childrenCount = 0;
        N::getChildren(tmp, 0u, 255u, children, childrenCount);
        for (uint32_t i = 0; i < childrenCount; ++i) {
            N *n = std::get<1>(children[i]);
            res += memory_profile(n);
        }
    }

    return res;
}