//
// Created by 王柯 on 5/25/21.
//

#include "roart_node.h"

#ifdef ROART_PROFILE_TIME
timeval start_time, end_time;
uint64_t _grow = 0, _update = 0, _travelsal = 0, _decompression = 0;
#endif

uint64_t roart_memory_usage = 0;

inline void mfence(void) {
    asm volatile("mfence":: :"memory");
}


inline void clflush(void *data, size_t len) {
    volatile char *ptr = (char *) ((unsigned long) data & (~(CACHELINESIZE - 1)));
    mfence();
    for (; ptr < (char *)data + len; ptr += CACHELINESIZE) {
        asm volatile("clflush %0" : "+m" (*(volatile char *) ptr));
    }
    mfence();
}

size_t size_align(size_t s, int align) {
    return ((s + align - 1) / align) * align;
}

size_t get_node_size(NTypes type) {
    switch (type) {
        case NTypes::N4:
            return sizeof(N4);
        case NTypes::N16:
            return sizeof(N16);
        case NTypes::N48:
            return sizeof(N48);
        case NTypes::N256:
            return sizeof(N256);
        case NTypes::ROART_Leaf:
            return sizeof(ROART_Leaf);
        case NTypes::LeafArray:
            return sizeof(LeafArray);
        default:
            std::cout << "[ALLOC NODE]\twrong type\n";
            assert(0);
    }
}

void *alloc_new_node_from_type(NTypes type) {

    size_t node_size = size_align(get_node_size(type), 64);
    void *addr = concurrency_fast_alloc(node_size);
    roart_memory_usage += get_node_size(type);

    return addr;
}


void *alloc_new_node_from_size(size_t size) {

    size_t node_size = size_align(size, 64);
    void *addr = concurrency_fast_alloc(node_size);
    roart_memory_usage += size;

    return addr;
}

ROART_Leaf::ROART_Leaf(uint64_t _key, uint64_t _value, uint8_t *_fkey) : BaseNode(NTypes::ROART_Leaf) {
    key_len = sizeof(_key);
    val_len = sizeof(_value);

#ifdef KEY_INLINE
    // have allocate the memory for kv
    memcpy(kv, k->fkey, key_len);
    memcpy(kv + key_len, (void *)k->value, val_len);
#else
    // allocate from NVM for variable key
    fkey = new (alloc_new_node_from_size(key_len)) uint8_t[key_len];
    value = new (alloc_new_node_from_size(val_len)) char[val_len];
    memcpy(fkey, _fkey, key_len);
    memcpy(value, (char *)&(_value), val_len);
    clflush((void *)fkey, key_len);
    clflush((void *)value, val_len);

    // persist the key, without persist the link to leaf
    // no one can see the key
    // if crash without link the leaf, key can be reclaimed safely
#endif
}


ROART_Leaf::ROART_Leaf(const ROART_KEY *k) : BaseNode(NTypes::ROART_Leaf) {
    key_len = k->key_len;
    val_len = k->val_len;
#ifdef KEY_INLINE
    // have allocate the memory for kv
    memcpy(kv, k->fkey, key_len);
    memcpy(kv + key_len, (void *)k->value, val_len);
#else
    // allocate from NVM for variable key
    fkey = new (alloc_new_node_from_size(key_len)) uint8_t[key_len];
    value = new (alloc_new_node_from_size(val_len)) char[val_len];
    memcpy(fkey, k->fkey, key_len);
    memcpy(value, (char *)&(k->value), val_len);
    clflush((void *)fkey, key_len);
    clflush((void *)value, val_len);

    // persist the key, without persist the link to leaf
    // no one can see the key
    // if crash without link the leaf, key can be reclaimed safely
#endif
}

// update value, so no need to alloc key
ROART_Leaf::ROART_Leaf(uint8_t *key_, size_t key_len_, char *value_, size_t val_len_)
        : BaseNode(NTypes::ROART_Leaf) {
    key_len = key_len_;
    val_len = val_len_;
#ifdef KEY_INLINE
    memcpy(kv, key_, key_len);
    memcpy(kv + key_len, value_, val_len);
#else
    fkey = key_; // no need to alloc a new key, key_ is persistent
    value = new (alloc_new_node_from_size(val_len)) char[val_len];
    memcpy(value, (void *)value_, val_len);
    clflush((void *)value, val_len);
#endif
}

uint16_t ROART_Leaf::getFingerPrint() {
    uint16_t re = 0;
    auto k = GetKey();
    for (int i = 0; i < key_len; i++) {
        re = re * 131 + k[i];
    }
    return re;
}

void ROART_Leaf::graphviz_debug(std::ofstream &f) {
//    char buf[1000] = {};
//    sprintf(buf + strlen(buf), "node%lx [label=\"",
//            reinterpret_cast<uintptr_t>(this));
//    sprintf(buf + strlen(buf), "ROART_Leaf\n");
//    sprintf(buf + strlen(buf), "ROART_KEY Len: %d\n", this->key_len);
//    for (int i = 0; i < this->key_len; i++) {
//        sprintf(buf + strlen(buf), "%c ", this->kv[i]);
//    }
//    sprintf(buf + strlen(buf), "\n");
//
//    sprintf(buf + strlen(buf), "Val Len: %d\n", this->val_len);
//    for (int i = 0; i < this->val_len; i++) {
//        sprintf(buf + strlen(buf), "%c ", this->kv[key_len + i]);
//    }
//    sprintf(buf + strlen(buf), "\n");
//    sprintf(buf + strlen(buf), "\"]\n");
//
//    f << buf;

    //    printf("leaf!");
}

void N::helpFlush(std::atomic<N *> *n) {
    if (n == nullptr)
        return;
    N *now_node = n->load();
    // printf("help\n");
    if (N::isDirty(now_node)) {
        //        printf("help, point to type is %d\n",
        //               ((BaseNode *)N::clearDirty(now_node))->type);
        clflush((void *)n, sizeof(N *));
        //        clflush((char *)n, sizeof(N *), true, true);
        n->compare_exchange_strong(now_node, N::clearDirty(now_node));
    }
}

void N::set_generation() {
//    NVMMgr *mgr = get_nvm_mgr();
//    generation_version = mgr->get_generation_version();
}

uint64_t N::get_generation() { return generation_version; }

#ifdef INSTANT_RESTART
void N::check_generation() {
    //    if(generation_version != 1100000){
    //        return;
    //    }else{
    //        generation_version++;
    //        return;
    //    }
    uint64_t mgr_generation = get_threadlocal_generation();

    uint64_t zero = 0;
    uint64_t one = 1;
    if (generation_version != mgr_generation) {
        //        printf("start to recovery of this node %lld\n",
        //        (uint64_t)this);
        if (recovery_latch.compare_exchange_strong(zero, one)) {
            //            printf("start to recovery of this node %lld\n",
            //            (uint64_t)this);
            type_version_lock_obsolete = new std::atomic<uint64_t>;
            type_version_lock_obsolete->store(convertTypeToVersion(type));
            type_version_lock_obsolete->fetch_add(0b100);

            count = 0;
            compactCount = 0;

            NTypes t = this->type;
            switch (t) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(this);
                for (int i = 0; i < 4; i++) {
#ifdef ZENTRY
                    if (n->zens[i].load() != 0) {
                        count++;
                        compactCount = i;
                    }
#else
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
#endif
                }
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(this);
                for (int i = 0; i < 16; i++) {
#ifdef ZENTRY
                    if (n->zens[i].load() != 0) {
                        count++;
                        compactCount = i;
                    }
#else
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
#endif
                }
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(this);
                for (int i = 0; i < 48; i++) {
#ifdef ZENTRY
                    auto p = getZentryKeyPtr(n->zens[i]);
                    if (p.second != nullptr) {
                        n->childIndex[p.first] = i;
                        count++;
                        compactCount = i;
                    }
#else
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
#endif
                }
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(this);
                for (int i = 0; i < 256; i++) {
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
                }
                break;
            }
            case NTypes::LeafArray: {
                auto n = static_cast<LeafArray *>(this);
                n->reload();
                break;
            }
            default: {
                std::cout << "[Recovery]\twrong type " << (int)type << "\n";
                assert(0);
            }
            }

            generation_version = mgr_generation;
            clflush(&generation_version, sizeof(uint64_t));
            recovery_latch.store(zero);

        } else {
            while (generation_version != mgr_generation) {
            }
        }
    }
}
#endif

void N::setType(NTypes type) {
    type_version_lock_obsolete->fetch_add(convertTypeToVersion(type));
}

uint64_t N::convertTypeToVersion(NTypes type) {
    return (static_cast<uint64_t>(type) << 60);
}

NTypes N::getType() const {
    return static_cast<NTypes>(
            type_version_lock_obsolete->load(std::memory_order_relaxed) >> 60);
}

uint32_t N::getLevel() const { return level; }

// wait to get lock, restart if obsolete
void N::writeLockOrRestart(bool &needRestart) {
    uint64_t version;
    do {
        version = type_version_lock_obsolete->load();
        while (isLocked(version)) {
            _mm_pause();
            version = type_version_lock_obsolete->load();
        }
        if (isObsolete(version)) {
            needRestart = true;
            return;
        }
    } while (!type_version_lock_obsolete->compare_exchange_weak(
            version, version + 0b10));
}

// restart if locked or obsolete or a different version
void N::lockVersionOrRestart(uint64_t &version, bool &needRestart) {
    if (isLocked(version) || isObsolete(version)) {
        needRestart = true;
        return;
    }
    if (type_version_lock_obsolete->compare_exchange_strong(version,
                                                            version + 0b10)) {
        version = version + 0b10;
    } else {
        needRestart = true;
    }
}

void N::writeUnlock() { type_version_lock_obsolete->fetch_add(0b10); }

N *N::getAnyChild(N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->getAnyChild();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->getAnyChild();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->getAnyChild();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->getAnyChild();
        }
        case NTypes::LeafArray: {
            auto n = static_cast<const LeafArray *>(node);
            return n->getAnyChild();
        }
        default: {
            assert(false);
        }
    }
    return nullptr;
}

void N::change(N *node, uint8_t key, N *val) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->change(key, val);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->change(key, val);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->change(key, val);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->change(key, val);
            return;
        }
        default: {
            assert(false);
        }
    }
}

template <typename curN, typename biggerN>
void N::tryInsertOrGrowAndUnlock(curN *n, N *parentNode, uint8_t keyParent,
                                 uint8_t key, N *val, NTypes type,
                                 bool &needRestart) {
#ifdef ROART_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
    if (n->insert(key, val, true)) {
        n->writeUnlock();
#ifdef ROART_PROFILE_TIME
        gettimeofday(&end_time, NULL);
        _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
        return;
    }

    // grow and lock parent
#ifdef ROART_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nBig);
        n->writeUnlock();
        return;
    }

    // allocate a bigger node from NVMMgr
#ifdef ARTPMDK
    biggerN *nBig = new (allocate_size(sizeof(biggerN)))
        biggerN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nBig = new (alloc_new_node_from_type(type))
            biggerN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->copyTo(nBig);               // not persist
    nBig->insert(key, val, false); // not persist
    // persist the node
    clflush((void *)nBig, sizeof(biggerN));
    //    clflush((char *)nBig, sizeof(biggerN), true, true);

    N::change(parentNode, keyParent, nBig);
    parentNode->writeUnlock();

    n->writeUnlockObsolete();
//    EpochGuard::DeleteNode((void *)n);
#ifdef ROART_PROFILE_TIME
    gettimeofday(&end_time, NULL);
    _grow += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
}

template <typename curN>
void N::compactAndInsertAndUnlock(curN *n, N *parentNode, uint8_t keyParent,
                                  uint8_t key, N *val, NTypes type,
                                  bool &needRestart) {
    // compact and lock parent
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nNew);
        n->writeUnlock();
        return;
    }

    // allocate a new node from NVMMgr
#ifdef ROART_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
#ifdef ARTPMDK
    curN *nNew = new (allocate_size(sizeof(curN)))
        curN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nNew = new (alloc_new_node_from_type(type))
            curN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->copyTo(nNew);               // not persist
    nNew->insert(key, val, false); // not persist
    // persist the node
    clflush((void *)nNew, sizeof(curN));
    //    clflush((char *)nNew, sizeof(curN), true, true);
#ifdef ROART_PROFILE_TIME
    gettimeofday(&end_time, NULL);
    _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
    N::change(parentNode, keyParent, nNew);
    parentNode->writeUnlock();

    n->writeUnlockObsolete();
//    EpochGuard::DeleteNode((void *)n);
}

void N::insertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key,
                        N *val, bool &needRestart) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            if (n->compactCount == 4 && n->count <= 3) {
                compactAndInsertAndUnlock<N4>(n, parentNode, keyParent, key, val,
                                              NTypes::N4, needRestart);
                break;
            }
            tryInsertOrGrowAndUnlock<N4, N16>(n, parentNode, keyParent, key, val,
                                              NTypes::N16, needRestart);
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            if (n->compactCount == 16 && n->count <= 14) {
                compactAndInsertAndUnlock<N16>(n, parentNode, keyParent, key, val,
                                               NTypes::N16, needRestart);
                break;
            }
            tryInsertOrGrowAndUnlock<N16, N48>(n, parentNode, keyParent, key, val,
                                               NTypes::N48, needRestart);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            if (n->compactCount == 48 && n->count != 48) {
                compactAndInsertAndUnlock<N48>(n, parentNode, keyParent, key, val,
                                               NTypes::N48, needRestart);
                break;
            }
            tryInsertOrGrowAndUnlock<N48, N256>(n, parentNode, keyParent, key, val,
                                                NTypes::N256, needRestart);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
#ifdef ROART_PROFILE_TIME
            gettimeofday(&start_time, NULL);
#endif
            n->insert(key, val, true);
            node->writeUnlock();
#ifdef ROART_PROFILE_TIME
            gettimeofday(&end_time, NULL);
            _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
            break;
        }
        default: {
            assert(false);
        }
    }
}

N *N::getChild(const uint8_t k, N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            return n->getChild(k);
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            return n->getChild(k);
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            return n->getChild(k);
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            return n->getChild(k);
        }
        default: {
            assert(false);
        }
    }
    return nullptr;
}

// only use in normally shutdown
void N::deleteChildren(N *node) {
    if (N::isLeaf(node)) {
        return;
    }
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->deleteChildren();
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->deleteChildren();
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->deleteChildren();
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->deleteChildren();
            return;
        }
        default: {
            assert(false);
        }
    }
}

template <typename curN, typename smallerN>
void N::removeAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key,
                        NTypes type, bool &needRestart) {
    if (n->remove(key, parentNode == nullptr, true)) {
        n->writeUnlock();
        return;
    }

    // shrink and lock parent
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nSmall);
        n->writeUnlock();
        return;
    }

    // allocate a smaller node from NVMMgr
#ifdef ARTPMDK
    smallerN *nSmall = new (allocate_size(sizeof(smallerN)))
        smallerN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nSmall = new (alloc_new_node_from_type(type))
            smallerN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->remove(key, true, true);
    n->copyTo(nSmall); // not persist

    // persist the node
    clflush((void *)nSmall, sizeof(smallerN));
    //    clflush((char *)nSmall, sizeof(smallerN), true, true);
    N::change(parentNode, keyParent, nSmall);

    parentNode->writeUnlock();
    n->writeUnlockObsolete();
//    EpochGuard::DeleteNode((void *)n);
}

void N::removeAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent,
                        bool &needRestart) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->remove(key, false, true);
            n->writeUnlock();
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            removeAndShrink<N16, N4>(n, parentNode, keyParent, key, NTypes::N4,
                                     needRestart);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            removeAndShrink<N48, N16>(n, parentNode, keyParent, key, NTypes::N16,
                                      needRestart);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            removeAndShrink<N256, N48>(n, parentNode, keyParent, key, NTypes::N48,
                                       needRestart);
            break;
        }
        default: {
            assert(false);
        }
    }
}

bool N::isLocked(uint64_t version) const { return ((version & 0b10) == 0b10); }

uint64_t N::getVersion() const { return type_version_lock_obsolete->load(); }

bool N::isObsolete(uint64_t version) { return (version & 1) == 1; }

bool N::checkOrRestart(uint64_t startRead) const {
    return readVersionOrRestart(startRead);
}

bool N::readVersionOrRestart(uint64_t startRead) const {
    return startRead == type_version_lock_obsolete->load();
}

void N::setCount(uint16_t count_, uint16_t compactCount_) {
    count = count_;
    compactCount = compactCount_;
}

// only invoked in the critical section
uint32_t N::getCount(N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<const N4 *>(node);
            return n->getCount();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16 *>(node);
            return n->getCount();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48 *>(node);
            return n->getCount();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256 *>(node);
            return n->getCount();
        }
        case NTypes::LeafArray: {
            auto n = static_cast<const LeafArray *>(node);
            return n->getCount();
        }
        default: {
            return 0;
        }
    }
}

Prefix N::getPrefi() const { return prefix.load(); }

void N::setPrefix(const uint8_t *prefix, uint32_t length, bool flush) {
    if (length > 0) {
        Prefix p;
        memcpy(p.prefix, prefix, std::min(length, maxStoredPrefixLength));
        p.prefixCount = length;
        this->prefix.store(p, std::memory_order_release);
    } else {
        Prefix p;
        p.prefixCount = 0;
        this->prefix.store(p, std::memory_order_release);
    }
    if (flush)
        clflush((void *)&(this->prefix), sizeof(Prefix));
}

void N::addPrefixBefore(N *node, uint8_t key) {
    Prefix p = this->getPrefi();
    Prefix nodeP = node->getPrefi();
    uint32_t prefixCopyCount =
            std::min(maxStoredPrefixLength, nodeP.prefixCount + 1);
    memmove(p.prefix + prefixCopyCount, p.prefix,
            std::min(p.prefixCount, maxStoredPrefixLength - prefixCopyCount));
    memcpy(p.prefix, nodeP.prefix,
           std::min(prefixCopyCount, nodeP.prefixCount));
    if (nodeP.prefixCount < maxStoredPrefixLength) {
        p.prefix[prefixCopyCount - 1] = key;
    }
    p.prefixCount += nodeP.prefixCount + 1;
    this->prefix.store(p, std::memory_order_release);
    clflush((void *)&(this->prefix), sizeof(Prefix));
}

bool N::isLeaf(const N *n) {
    return (reinterpret_cast<uintptr_t>(n) & (1ULL << 0));
}

N *N::setLeaf(const ROART_Leaf *k) {
    return reinterpret_cast<N *>(reinterpret_cast<void *>(
            (reinterpret_cast<uintptr_t>(k) | (1ULL << 0))));
}

ROART_Leaf *N::getLeaf(const N *n) {
    return reinterpret_cast<ROART_Leaf *>(reinterpret_cast<void *>(
            (reinterpret_cast<uintptr_t>(n) & ~(1ULL << 0))));
}

bool N::isLeafArray(const N *n) {
    return (reinterpret_cast<uintptr_t>(n) & (1ULL << 0)) == 1;
}

LeafArray *N::getLeafArray(const N *n) {
    return reinterpret_cast<LeafArray *>(
            (reinterpret_cast<uintptr_t>(n) & ~(1ULL << 0)));
}

N *N::setLeafArray(const LeafArray *la) {
    return reinterpret_cast<N *>(
            (reinterpret_cast<uintptr_t>(la) | (1ULL << 0)));
}

// only invoke this in remove and N4
std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            return n->getSecondChild(key);
        }
        default: {
            assert(false);
        }
    }
}

// only invoke in the shutdown normally
void N::deleteNode(N *node) {
    if (N::isLeaf(node)) {
        return;
    }
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            delete n;
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            delete n;
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            delete n;
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            delete n;
            return;
        }
        default: {
            assert(false);
        }
    }
    delete node;
}

// invoke in the insert
// not all nodes are in the critical secton
ROART_Leaf *N::getAnyChildTid(const N *n) {
    const N *nextNode = n;

    while (true) {
        N *node = const_cast<N *>(nextNode);
        nextNode = getAnyChild(node);

        assert(nextNode != nullptr);
        if (isLeaf(nextNode)) {
            return getLeaf(nextNode);
        }
    }
}

// for range query
void N::getChildren(N *node, uint8_t start, uint8_t end,
                    std::tuple<uint8_t, N *> children[],
                    uint32_t &childrenCount) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->getChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->getChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->getChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->getChildren(start, end, children, childrenCount);
            return;
        }
        default: {
            assert(false);
        }
    }
}

void N::rebuild_node(N *node, std::vector<std::pair<uint64_t, size_t>> &rs,
                     uint64_t start_addr, uint64_t end_addr, int thread_id) {
    if (N::isLeaf(node)) {
        // leaf node
#ifdef RECLAIM_MEMORY
        ROART_Leaf *leaf = N::getLeaf(node);
        if ((uint64_t)leaf < start_addr || (uint64_t)leaf >= end_addr)
            return;
#ifdef KEY_INLINE
        size_t size =
            size_align(sizeof(ROART_Leaf) + leaf->key_len + leaf->val_len, 64);
        //        size = convert_power_two(size);
        rs.push_back(std::make_pair((uint64_t)leaf, size));
#else
        NTypes type = leaf->type;
        size_t size = size_align(get_node_size(type), 64);
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)leaf, size));

        // leaf key also need to insert into rs set
        size = leaf->key_len;
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)(leaf->fkey), size));

        // value
        size = leaf->val_len;
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)(leaf->value), size));
#endif // KEY_INLINE

#endif
        return;
    }
    // insert internal node into set
    NTypes type = node->type;
#ifdef RECLAIM_MEMORY
    if ((uint64_t)node >= start_addr && (uint64_t)node < end_addr) {
        size_t size = size_align(get_node_size(type), 64);
        //    size = convert_power_two(size);
        rs.push_back(std::make_pair((uint64_t)node, size));
    }
#endif

    int xcount = 0;
    int xcompactCount = 0;
    // type is persistent when first create this node
    // TODO: using SIMD to accelerate recovery
    switch (type) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            for (int i = 0; i < 4; i++) {
#ifdef ZENTRY
                N *child = getZentryPtr(n->zens[i]);
#else
                N *child = n->children[i].load();
#endif
                if (child != nullptr) {
                    xcount++;
                    xcompactCount = i;
                    rebuild_node(child, rs, start_addr, end_addr, thread_id);
                }
            }
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            for (int i = 0; i < 16; i++) {
#ifdef ZENTRY
                N *child = getZentryPtr(n->zens[i]);
#else
                N *child = n->children[i].load();
#endif
                if (child != nullptr) {
                    xcount++;
                    xcompactCount = i;
                    rebuild_node(child, rs, start_addr, end_addr, thread_id);
                }
            }
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            for (int i = 0; i < 48; i++) {
#ifdef ZENTRY
                N *child = getZentryPtr(n->zens[i]);
#else
                N *child = n->children[i].load();
#endif
                if (child != nullptr) {
                    xcount++;
                    xcompactCount = i;
                    rebuild_node(child, rs, start_addr, end_addr, thread_id);
                }
            }
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            for (int i = 0; i < 256; i++) {
                N *child = n->children[i].load();
                if (child != nullptr) {
                    xcount++;
                    xcompactCount = i;
                    rebuild_node(child, rs, start_addr, end_addr, thread_id);
                }
            }
            break;
        }
        default: {
            std::cout << "[Rebuild]\twrong type is " << (int)type << "\n";
            assert(0);
        }
    }
    // reset count and version and lock
#ifdef INSTANT_RESTART
#else
    if ((uint64_t)node >= start_addr && (uint64_t)node < end_addr) {
        node->setCount(xcount, xcompactCount);
        node->type_version_lock_obsolete = new std::atomic<uint64_t>;
        node->type_version_lock_obsolete->store(convertTypeToVersion(type));
        node->type_version_lock_obsolete->fetch_add(0b100);
//        node->old_pointer.store(0);
        //        rs.push_back(std::make_pair(0,0));
    }
#endif
}
void N::graphviz_debug(std::ofstream &f, N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            n->graphviz_debug(f);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            n->graphviz_debug(f);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            n->graphviz_debug(f);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            n->graphviz_debug(f);
            return;
        }
        default: {
            assert(false);
        }
    }
}

void N::unchecked_insert(N *node, uint8_t roart_key_byte, N *child, bool flush) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
        case NTypes::N4: {
            auto n = static_cast<N4 *>(node);
            auto re = n->insert(roart_key_byte, child, flush);
            assert(re);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16 *>(node);
            auto re = n->insert(roart_key_byte, child, flush);
            assert(re);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48 *>(node);
            auto re = n->insert(roart_key_byte, child, flush);
            assert(re);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256 *>(node);
            auto re = n->insert(roart_key_byte, child, flush);
            assert(re);
            return;
        }
        default: {
            assert(false);
        }
    }
}

bool N::key_keylen_lt(uint8_t *a, const int alen, uint8_t *b, const int blen,
                      const int compare_level) {
    for (int i = compare_level; i < std::min(alen, blen); i++) {
        if (a[i] != b[i]) {
            return a[i] < b[i];
        }
    }
    return alen < blen;
}
bool N::leaf_lt(ROART_Leaf *a, ROART_Leaf *b, int compare_level) {
    return key_keylen_lt((uint8_t *)a->GetKey(), a->key_len, (uint8_t *)b->GetKey(), b->key_len,
                         compare_level);
}
bool N::leaf_key_lt(ROART_Leaf *a, const ROART_KEY *b, const int compare_level) {
    return key_keylen_lt((uint8_t *)a->GetKey(), a->key_len,
                         (uint8_t *)(b->fkey), b->key_len,
                         compare_level);
}
bool N::key_leaf_lt(const ROART_KEY *a, ROART_Leaf *b, const int compare_level) {
    return key_keylen_lt((uint8_t *)(a->fkey), a->key_len,
                         (uint8_t *)b->GetKey(), b->key_len, compare_level);
}
bool N::key_key_lt(const ROART_KEY *a, const ROART_KEY *b) {
    return key_keylen_lt((uint8_t *)(a->fkey), a->key_len,
                         (uint8_t *)(b->fkey), b->key_len, 0);
}
uint8_t N::getZentryKey(uintptr_t zentry) {
    return uint8_t(zentry >> ZentryKeyShift);
}

N *N::getZentryPtr(uintptr_t zentry) {
    const uintptr_t mask = (1LL << ZentryKeyShift) - 1;
    return reinterpret_cast<N *>(zentry & mask);
}
std::pair<uint8_t, N *> N::getZentryKeyPtr(uintptr_t zentry) {
    return {getZentryKey(zentry), getZentryPtr(zentry)};
}
uintptr_t N::makeZentry(uint8_t key, N *node) {
    return (uintptr_t(key) << ZentryKeyShift) |
           reinterpret_cast<uintptr_t>(node);
}

void N4::deleteChildren() {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        N *child = N::clearDirty(getZentryPtr(zens[i]));
#else
        N *child = N::clearDirty(children[i].load());
#endif
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N4::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 4) {
        return false;
    }
#ifdef ZENTRY
    zens[compactCount].store(makeZentry(key, n));
    if (flush)
        clflush(&zens[compactCount], sizeof(std::atomic<uintptr_t>));

#else
    keys[compactCount].store(key, std::memory_order_seq_cst);
    if (flush)
        clflush((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush) {
        clflush((void *)&children[compactCount], sizeof(std::atomic<N *>));
    }
#endif
    compactCount++;
    count++;
    return true;
}

void N4::change(uint8_t key, N *val) {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == key) {
            zens[i].store(makeZentry(key, val));
            clflush((void *)&zens[i], sizeof(std::atomic<uintptr_t>));
            return;
        }
#else
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == key) {
            children[i].store(val, std::memory_order_seq_cst);
            clflush((void *)&children[i], sizeof(std::atomic<N *>));
            return;
        }
#endif
    }
}

N *N4::getChild(const uint8_t k) {
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == k) {
            return p.second;
        }
#else
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == k) {
            return child;
        }
#endif
    }
    return nullptr;
}

bool N4::remove(uint8_t k, bool force, bool flush) {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == k) {
            zens[i].store(0);
            clflush(&zens[i], sizeof(std::atomic<uintptr_t>));
            count--;
            return true;
        }
#else
        if (children[i] != nullptr && keys[i].load() == k) {
            children[i].store(nullptr, std::memory_order_seq_cst);
            clflush((void *)&children[i], sizeof(std::atomic<N *>));
            count--;
            return true;
        }
#endif
    }
    return false;
}

N *N4::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        N *child = getZentryPtr(zens[i].load());
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
#else
        N *child = children[i].load();
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
#endif
    }
    return anyChild;
}

// in the critical section
std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr) {
            if (p.first != key) {
                return std::make_tuple(p.second, p.first);
            }
        }
#else
        N *child = children[i].load();
        if (child != nullptr) {
            uint8_t k = keys[i].load();
            if (k != key) {
                return std::make_tuple(child, k);
            }
        }
#endif
    }
    return std::make_tuple(nullptr, 0);
}

// must read persisted child
void N4::getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount) {
    childrenCount = 0;
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i]);
        if (p.first >= start && p.first <= end) {
            if (p.second != nullptr) {
                children[childrenCount] = std::make_tuple(p.first, p.second);
                childrenCount++;
            }
        }
#else
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(key, child);
                childrenCount++;
            }
        }
#endif
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) {
                  return std::get<0>(first) < std::get<0>(second);
              });
}

uint32_t N4::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
#ifdef ZENTRY
        N *child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
void N4::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N4 %d\n", level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto pp = getZentryKeyPtr(zens[i].load());
        auto p = pp.second;
        auto x = pp.first;
#else
        auto p = children[i].load();
        auto x = keys[i].load();
#endif
        if (p != nullptr) {

            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto p = getZentryPtr(zens[i].load());
#else
        auto p = children[i].load();
#endif
        if (p != nullptr) {
            if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                auto la = getLeafArray(p);
                la->graphviz_debug(f);
#else
                auto l = getLeaf(p);
                l->graphviz_debug(f);
#endif
            } else {
                N::graphviz_debug(f, p);
            }
        }
    }
}

bool N16::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 16) {
        return false;
    }

#ifdef ZENTRY
    zens[compactCount].store(makeZentry(flipSign(key), n));
    if (flush)
        clflush(&zens[compactCount], sizeof(std::atomic<uintptr_t>));
#else
    keys[compactCount].store(flipSign(key), std::memory_order_seq_cst);
    if (flush)
        clflush((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush)
        clflush((void *)&children[compactCount], sizeof(std::atomic<N *>));
#endif
    compactCount++;
    count++;
    return true;
}

void N16::change(uint8_t key, N *val) {
    auto childPos = getChildPos(key);
    assert(childPos != -1);
#ifdef ZENTRY
    zens[childPos].store(makeZentry(flipSign(key), val));
    clflush(&zens[childPos], sizeof(std::atomic<uintptr_t>));
#else
    children[childPos].store(val, std::memory_order_seq_cst);
    clflush(&children[childPos], sizeof(std::atomic<N *>));
#endif
}

int N16::getChildPos(const uint8_t k) {
#ifdef ZENTRY
    uint8_t keys[16] = {};
    for (int i = 0; i < 16; i++) {
        keys[i] = getZentryKey(zens[i].load());
    }
    __m128i cmp = _mm_cmpeq_epi8(
            _mm_set1_epi8(flipSign(k)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);
        if (getZentryPtr(zens[pos]) != nullptr) {
            return pos;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return -1;
#else
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);
        if (children[pos].load() != nullptr) {
            return pos;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return -1;
#endif
}

N *N16::getChild(const uint8_t k) {
#ifdef ZENTRY
    uint8_t keys[16] = {};
    for (int i = 0; i < 16; i++) {
        keys[i] = getZentryKey(zens[i].load());
    }
    __m128i cmp = _mm_cmpeq_epi8(
            _mm_set1_epi8(flipSign(k)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);
        auto p = getZentryKeyPtr(zens[pos]);

        if (p.second != nullptr && p.first == flipSign(k)) {
            return p.second;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
#else
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        N *child = children[pos].load();
        if (child != nullptr && keys[pos].load() == flipSign(k)) {
            return child;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
#endif
}

bool N16::remove(uint8_t k, bool force, bool flush) {
    if (count <= 3 && !force) {
        return false;
    }
    auto leafPlace = getChildPos(k);
    assert(leafPlace != -1);
#ifdef ZENTRY
    zens[leafPlace].store(0);
    clflush(&zens[leafPlace], sizeof(std::atomic<uintptr_t>));
#else
    children[leafPlace].store(nullptr, std::memory_order_seq_cst);
    clflush((void *)&children[leafPlace], sizeof(std::atomic<N *>));
#endif
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N16::getAnyChild() const {
    N *anyChild = nullptr;
    for (int i = 0; i < 16; ++i) {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
    }
    return anyChild;
}

void N16::deleteChildren() {
    for (std::size_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        N *child = N::clearDirty(getZentryPtr(zens[i].load()));
#else
        N *child = N::clearDirty(children[i].load());
#endif
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

void N16::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (int i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        p.first = flipSign(p.first);
        if (p.first >= start && p.first <= end) {
            if (p.second != nullptr) {
                children[childrenCount] = std::make_tuple(p.first, p.second);
                childrenCount++;
            }
        }
#else
        uint8_t key = flipSign(this->keys[i].load());
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(key, child);
                childrenCount++;
            }
        }
#endif
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) {
                  return std::get<0>(first) < std::get<0>(second);
              });
}

uint32_t N16::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr)
            ++cnt;
    }
    return cnt;
}
void N16::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N16 %d\n", level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto pp = getZentryKeyPtr(zens[i].load());
        auto p = pp.second;
        auto x = pp.first;
#else
        auto p = children[i].load();
        auto x = keys[i].load();
#endif
        if (p != nullptr) {
            x = flipSign(x);
            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto p = getZentryPtr(zens[i].load());
#else
        auto p = children[i].load();
#endif
        if (p != nullptr) {
            if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                auto la = getLeafArray(p);
                la->graphviz_debug(f);
#else
                auto l = getLeaf(p);
                l->graphviz_debug(f);
#endif
            } else {
                N::graphviz_debug(f, p);
            }
        }
    }
}


bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }

#ifdef ZENTRY
    childIndex[key].store(compactCount, std::memory_order_seq_cst);

    zens[compactCount].store(makeZentry(key, n));
    if (flush) {
        clflush(&zens[compactCount], sizeof(std::atomic<uintptr_t>));
    }
#else
    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        clflush((void *)&childIndex[key], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush) {
        clflush((void *)&children[compactCount], sizeof(std::atomic<N *>));
    }
#endif

    compactCount++;
    count++;
    return true;
}

void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);
#ifdef ZENTRY
    zens[index].store(makeZentry(key, val));
    clflush(&zens[index], sizeof(std::atomic<uintptr_t>));
#else
    children[index].store(val, std::memory_order_seq_cst);
    clflush((void *)&children[index], sizeof(std::atomic<N *>));
#endif
}

N *N48::getChild(const uint8_t k) {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
#else
        N *child = children[index].load();
#endif
        return child;
    }
}

bool N48::remove(uint8_t k, bool force, bool flush) {
    if (count <= 12 && !force) {
        return false;
    }
    uint8_t index = childIndex[k].load();
    assert(index != emptyMarker);
#ifdef ZENTRY
    zens[index].store(0);
    clflush(&zens[index], sizeof(std::atomic<uintptr_t>));
#else
    children[index].store(nullptr, std::memory_order_seq_cst);
    clflush((void *)&children[index], sizeof(std::atomic<N *>));
#endif
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N48::getAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < 48; i++) {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
    }
    return anyChild;
}

void N48::deleteChildren() {
    for (unsigned i = 0; i < 256; i++) {
        uint8_t index = childIndex[i].load();
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
        if (index != emptyMarker && child != nullptr) {
            N *child = N::clearDirty(child);
            N::deleteChildren(child);
            N::deleteNode(child);
        }
#else
        if (index != emptyMarker && children[index].load() != nullptr) {
            N *child = N::clearDirty(children[index].load());
            N::deleteChildren(child);
            N::deleteNode(child);
        }
#endif
    }
}

void N48::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
        if (index != emptyMarker && child != nullptr) {
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }
#else
        if (index != emptyMarker && this->children[index] != nullptr) {
            N *child = this->children[index].load();

            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }
#endif
    }
}

uint32_t N48::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        uint8_t index = childIndex[i].load();
#ifdef ZENTRY
        if (index != emptyMarker && getZentryPtr(zens[index].load()) != nullptr)
            cnt++;
#else
        if (index != emptyMarker && children[index].load() != nullptr)
            cnt++;
#endif
    }
    return cnt;
}
void N48::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N48 %d\n", level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
#ifdef ZENTRY
            auto pp = getZentryKeyPtr(zens[ci].load());
            auto p = pp.second;
            auto x = pp.first;
#else
            auto p = children[i].load();
            auto x = ci;
#endif
            if (p != nullptr) {
                auto addr = reinterpret_cast<uintptr_t>(p);
                if (isLeaf(p)) {
                    addr = reinterpret_cast<uintptr_t>(getLeaf(p));
                }
                sprintf(buf + strlen(buf),
                        "node%lx -- node%lx [label=\"%c\"]\n",
                        reinterpret_cast<uintptr_t>(this), addr, x);
            }
        }
    }
    f << buf;

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
#ifdef ZENTRY
            auto p = getZentryPtr(zens[ci].load());
#else
            auto p = children[ci].load();
#endif
            if (p != nullptr) {
                if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                    auto la = getLeafArray(p);
                    la->graphviz_debug(f);
#else
                    auto l = getLeaf(p);
                    l->graphviz_debug(f);
#endif
                } else {
                    N::graphviz_debug(f, p);
                }
            }
        }
    }
}


void N256::deleteChildren() {
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N256::insert(uint8_t key, N *val, bool flush) {
    if (flush) {
        uint64_t oldp = (1ull << 56) | ((uint64_t)key << 48);
    }

    children[key].store(val, std::memory_order_seq_cst);
    if (flush) {
        clflush((void *)&children[key], sizeof(std::atomic<N *>));
    }

    count++;
    return true;
}

void N256::change(uint8_t key, N *n) {

    children[key].store(n, std::memory_order_seq_cst);
    clflush((void *)&children[key], sizeof(std::atomic<N *>));
}

N *N256::getChild(const uint8_t k) {
    N *child = children[k].load();
    return child;
}

bool N256::remove(uint8_t k, bool force, bool flush) {
    if (count <= 37 && !force) {
        return false;
    }

    children[k].store(nullptr, std::memory_order_seq_cst);
    clflush((void *)&children[k], sizeof(std::atomic<N *>));
    count--;
    return true;
}

N *N256::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = children[i].load();

        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
    }
    return anyChild;
}

void N256::getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, N *> children[],
                       uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        N *child = this->children[i].load();

        if (child != nullptr) {
            children[childrenCount] = std::make_tuple(i, child);
            childrenCount++;
        }
    }
}

uint32_t N256::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
void N256::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N256 %d\n",level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (auto i = 0; i < 256; i++) {
        auto p = children[i].load();
        if (p != nullptr) {
            auto x = i;
            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (auto &i : children) {
        auto p = i.load();
        if (p != nullptr) {
            if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                auto la = getLeafArray(p);
                la->graphviz_debug(f);
#else
                auto l = getLeaf(p);
                l->graphviz_debug(f);
#endif
            } else {
                N::graphviz_debug(f, p);
            }
        }
    }
}

inline uint64_t bitmap_find_first(bitset<LeafArrayLength> bitmap) {
    for (int i = 0; i < LeafArrayLength; ++i) {
        if (bitmap[i] == 1)
            return i;
    }
    return LeafArrayLength;
}

inline uint64_t bitmap_find_next(bitset<LeafArrayLength> bitmap, uint64_t cur) {
    for (int i = cur + 1; i < LeafArrayLength; ++i) {
        if (bitmap[i] == 1)
            return i;
    }
    return LeafArrayLength;
}

size_t LeafArray::getRightmostSetBit() const {
    auto b = bitmap.load();
#ifdef __linux__
    auto pos = b._Find_first();
#else
    auto pos = bitmap_find_first(b);
#endif
    assert(pos < LeafArrayLength);
    return pos;
}

void LeafArray::setBit(size_t bit_pos, bool to) {
    auto b = bitmap.load();
    b[bit_pos] = to;
    bitmap.store(b);
}

uint16_t LeafArray::getFingerPrint(size_t pos) const {
    auto x = reinterpret_cast<uint64_t>(leaf[pos].load());
    uint16_t re = x >> FingerPrintShift;
    return re;
}

ROART_Leaf *LeafArray::lookup(const ROART_KEY *k) const {
    uint16_t finger_print = k->getFingerPrint();

    auto b = bitmap.load();

#ifdef FIND_FIRST
    auto i = b[0] ? 0 : 1;
while (i < LeafArrayLength) {
    auto fingerprint_ptr = this->leaf[i].load();
    if (fingerprint_ptr != 0) {
        uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
        auto ptr = reinterpret_cast<ROART_Leaf *>(
            fingerprint_ptr ^
            (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
        if (finger_print == thisfp && ptr->checkKey(k)) {
            return ptr;
        }
    }
#ifdef __linux__
    i = b._Find_next(i);
#else
    i = bitmap_find_next(b, i);
#endif
}
#else
    for (int i = 0; i < LeafArrayLength; i++) {
        if (b[i] == false)
            continue;
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                return ptr;
            }
        }
    }
#endif

    return nullptr;
}

ROART_Leaf *LeafArray::mylookup(uint64_t _key, unsigned long _key_len, uint8_t *_fkey) const {
    uint16_t finger_print = 0;
    for (int i = 0; i < _key_len; i++) {
        finger_print = finger_print * 131 + _fkey[i];
    }

    auto b = bitmap.load();

#ifdef FIND_FIRST
    auto i = b[0] ? 0 : 1;
while (i < LeafArrayLength) {
    auto fingerprint_ptr = this->leaf[i].load();
    if (fingerprint_ptr != 0) {
        uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
        auto ptr = reinterpret_cast<ROART_Leaf *>(
            fingerprint_ptr ^
            (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
        if (finger_print == thisfp && ptr->mycheckKey(_key_len, _fkey)) {
            return ptr;
        }
    }
#ifdef __linux__
    i = b._Find_next(i);
#else
    i = bitmap_find_next(b, i);
#endif
}
#else
    for (int i = 0; i < LeafArrayLength; i++) {
        if (b[i] == false)
            continue;
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->mycheckKey(_key_len, _fkey)) {
                return ptr;
            }
        }
    }
#endif

    return nullptr;
}

bool LeafArray::insert(ROART_Leaf *l, bool flush) {
    auto b = bitmap.load();
    b.flip();
#ifdef __linux__
    auto pos = b._Find_first();
#else
    auto pos = bitmap_find_first(b);
#endif
    if (pos < LeafArrayLength) {
        b.flip();
        b[pos] = true;
        bitmap.store(b);
        auto s =
                (static_cast<uintptr_t>(l->getFingerPrint()) << FingerPrintShift) |
                (reinterpret_cast<uintptr_t>(l));
#ifdef ROART_PROFILE_TIME
        gettimeofday(&start_time, NULL);
#endif
        leaf[pos].store(s);
        if (flush)
            clflush((void *) &leaf[pos], sizeof(std::atomic<uintptr_t>));
#ifdef ROART_PROFILE_TIME
        gettimeofday(&end_time, NULL);
        _update += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
        return true;
    } else {
        return false;
    }
}

bool LeafArray::remove(const ROART_KEY *k) {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                leaf[i].store(0);
                clflush(&leaf[i], sizeof(std::atomic<uintptr_t>));
//                    EpochGuard::DeleteNode(ptr);
                b[i] = false;
                bitmap.store(b);
                return true;
            }
        }
#ifdef __linux__
        i = b._Find_next(i);
#else
        i = bitmap_find_next(b, i);
#endif
    }
    return false;
}

void LeafArray::reload() {
    auto b = bitmap.load();
    for (int i = 0; i < LeafArrayLength; i++) {
        if (leaf[i].load() != 0) {
            b[i] = true;
        } else {
            b[i] = false;
        }
    }
    bitmap.store(b);
}

void LeafArray::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "LeafArray\n");
    sprintf(buf + strlen(buf), "count: %zu\n", bitmap.load().count());
    sprintf(buf + strlen(buf), "\"]\n");

    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));

            sprintf(buf + strlen(buf), "node%lx -- node%lx \n",
                    reinterpret_cast<uintptr_t>(this),
                    reinterpret_cast<uintptr_t>(ptr));
        }
#ifdef __linux__
        i = b._Find_next(i);
#else
        i = bitmap_find_next(b, i);
#endif
    }

    f << buf;

    b = bitmap.load();
    i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));

            ptr->graphviz_debug(f);
        }
#ifdef __linux__
        i = b._Find_next(i);
#else
        i = bitmap_find_next(b, i);
#endif
    }
}

void LeafArray::splitAndUnlock(N *parentNode, uint8_t parentKey,
                               bool &need_restart) {

    parentNode->writeLockOrRestart(need_restart);

    if (need_restart) {
        this->writeUnlock();
        return;
    }

    auto b = bitmap.load();
    auto leaf_count = b.count();
    std::vector<char *> keys;
    //    char **keys = new char *[leaf_count];
    std::vector<int> lens;
    //    int *lens = new int[leaf_count];

    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            keys.push_back(ptr->GetKey());
            lens.push_back(ptr->key_len);
        }
#ifdef __linux__
        i = b._Find_next(i);
#else
        i = bitmap_find_next(b, i);
#endif
    }
    //    printf("spliting\n");

    std::vector<char> common_prefix;
    int level = 0;
    level = parentNode->getLevel() + 1;
    // assume keys are not substring of another key

    // todo: get common prefix can be optimized by binary search
    while (true) {
        bool out = false;
        for (i = 0; i < leaf_count; i++) {
            if (level < lens[i]) {
                if (i == 0) {
                    common_prefix.push_back(keys[i][level]);
                } else {
                    if (keys[i][level] != common_prefix.back()) {

                        common_prefix.pop_back();

                        out = true;
                        break;
                    }
                }
            } else {
                // assume keys are not substring of another key
                assert(0);
            }
        }
        if (out)
            break;
        level++;
    }
    std::map<char, LeafArray *> split_array;
    for (i = 0; i < leaf_count; i++) {
        if (split_array.count(keys[i][level]) == 0) {
            split_array[keys[i][level]] =
                    new(alloc_new_node_from_type(NTypes::LeafArray))
                            LeafArray(level);
        }
        split_array.at(keys[i][level])->insert(getLeafAt(i), false);
    }

    N *n;
    uint8_t *prefix_start = reinterpret_cast<uint8_t *>(common_prefix.data());
    auto prefix_len = common_prefix.size();
    auto leaf_array_count = split_array.size();
#ifdef ROART_PROFILE_TIME
    gettimeofday(&start_time, NULL);
#endif
    if (leaf_array_count <= 4) {
        n = new(alloc_new_node_from_type(NTypes::N4))
                N4(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 4 && leaf_array_count <= 16) {
        n = new(alloc_new_node_from_type(NTypes::N16))
                N16(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 16 && leaf_array_count <= 48) {
        n = new(alloc_new_node_from_type(NTypes::N48))
                N48(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 48 && leaf_array_count <= 256) {
        n = new(alloc_new_node_from_type(NTypes::N256))
                N256(level, prefix_start, prefix_len);
    } else {
        assert(0);
    }
    for (const auto &p : split_array) {
        unchecked_insert(n, p.first, setLeafArray(p.second), true);
        clflush(p.second, sizeof(LeafArray));
    }

    N::change(parentNode, parentKey, n);
    parentNode->writeUnlock();
#ifdef ROART_PROFILE_TIME
    gettimeofday(&end_time, NULL);
    _grow += (end_time.tv_sec - start_time.tv_sec) * 1000000 + end_time.tv_usec - start_time.tv_usec;
#endif
    this->writeUnlockObsolete();
//        EpochGuard::DeleteNode(this);
}

ROART_Leaf *LeafArray::getLeafAt(size_t pos) const {
    auto t = reinterpret_cast<uint64_t>(this->leaf[pos].load());
    t = (t << 16) >> 16;
    return reinterpret_cast<ROART_Leaf *>(t);
}

uint32_t LeafArray::getCount() const { return bitmap.load().count(); }

bool LeafArray::isFull() const { return getCount() == LeafArrayLength; }

std::vector<ROART_Leaf *> LeafArray::getSortedLeaf(const ROART_KEY *start, const ROART_KEY *end,
                                                   int start_level,
                                                   bool compare_start,
                                                   bool compare_end) {
    std::vector<ROART_Leaf *> leaves;
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;

    while (i < LeafArrayLength) {
        auto ptr = getLeafAt(i);
#ifdef __linux__
        i = b._Find_next(i);
#else
        i = bitmap_find_next(b, i);
#endif
        // start <= ptr < end
        if (compare_start) {
            auto lt_start = leaf_key_lt(ptr, start, start_level);
            if (lt_start == true) {
                continue;
            }
        }
        if (compare_end) {
            auto lt_end = leaf_key_lt(ptr, end, start_level);
            if (lt_end == false) {
                continue;
            }
        }
        leaves.push_back(ptr);
    }
#ifdef SORT_LEAVES
    std::sort(leaves.begin(), leaves.end(),
          [start_level](ROART_Leaf *a, ROART_Leaf *b) -> bool {
              leaf_lt(a, b, start_level);
          });
#endif
    return leaves;
}

bool LeafArray::update(const ROART_KEY *k, ROART_Leaf *l) {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();

#ifdef FIND_FIRST
    auto i = b[0] ? 0 : 1;
while (i < LeafArrayLength) {
    auto fingerprint_ptr = this->leaf[i].load();
    if (fingerprint_ptr != 0) {
        uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
        auto ptr = reinterpret_cast<ROART_Leaf *>(
            fingerprint_ptr ^
            (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
        if (finger_print == thisfp && ptr->checkKey(k)) {
            auto news = fingerPrintLeaf(finger_print, l);
            leaf[i].store(news);
            clflush(&leaf[i], sizeof(std::atomic<uintptr_t>));
            return true;
        }
    }
#ifdef __linux__
    i = b._Find_next(i);
#else
    i = bitmap_find_next(b, i);
#endif
}
#else
    for (int i = 0; i < LeafArrayLength; i++) {
        if (b[i] == false)
            continue;
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<ROART_Leaf *>(
                    fingerprint_ptr ^
                    (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                auto news = fingerPrintLeaf(finger_print, l);
                leaf[i].store(news);
                clflush(&leaf[i], sizeof(std::atomic<uintptr_t>));
                return true;
            }
        }
    }
#endif
    return false;
}

uintptr_t LeafArray::fingerPrintLeaf(uint16_t fingerPrint, ROART_Leaf *l) {
    uintptr_t mask = (1LL << FingerPrintShift) - 1;
    auto f = uintptr_t(fingerPrint);
    return (reinterpret_cast<uintptr_t>(l) & mask) | (f << FingerPrintShift);
}

N *LeafArray::getAnyChild() const {
    auto b = bitmap.load();
#ifdef __linux__
    auto i = b._Find_first();
#else
    auto i = bitmap_find_first(b);
#endif
    if (i == LeafArrayLength) {
        return nullptr;
    } else {
        return N::setLeaf(getLeafAt(i));
    }
}