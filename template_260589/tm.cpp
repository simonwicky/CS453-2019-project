/**
 * @file   tm.c
 * @author Simon Wicky <simon.wicky@epfl.ch
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <shared_mutex>
#include <mutex>
// Internal headers
#include <tm.hpp>

#include <iostream>
using namespace std;

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

// -------------------------------------------------------------------------- //

struct segment {
    shared_mutex lock;
    byte* mem;
    size_t size;
    bool freed;
};

struct region {
    void* start;
    vector<struct segment*> segments;
    size_t size;
    size_t align;
};

struct log{
    size_t size;
    void* location;
    void* old_data;
};

struct transaction {
    vector<struct log*> logs;
    vector<struct segment*> to_free;
    vector<shared_mutex*> to_free_locks;
    vector<struct segment*> new_segments;
    vector<shared_mutex*> new_seg_locks;
    struct region* region;
    bool is_ro;
    vector<shared_mutex*> locks;
    vector<shared_mutex*> read_locks;
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept{
    struct region* region = new (std::nothrow) struct region();
    if (unlikely(region == NULL)) {
        return invalid_shared;
    }

    struct segment* seg = new (std::nothrow) struct segment();
    if (unlikely(seg == NULL)) {
        delete region;
        return invalid_shared;
    }

    if (unlikely(posix_memalign(&(region->start), align, size) != 0)){
        delete seg;
        delete region ;
        return invalid_shared;
    }

    memset(region->start, 0, size);
    region->align = align;
    region->size = size;
    seg->mem = (byte*) region->start;
    seg->size = size;
    seg->freed = false;
    region->segments.push_back(seg);

    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    struct region* region = (struct region*) shared;
    for (auto seg : region->segments){
        free(seg->mem);
        delete seg;
    }
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((struct region*) shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    return ((struct region*) shared)->align;
}

//================================================================
//Helper functions
//================================================================
void free_segments(tx_t tx, vector<segment*> to_free){
    struct transaction * trans = (struct transaction*) tx;
    for(auto seg_to_free : to_free){
        int index = 0;
        for(auto seg : trans->region->segments){
            //find segment to free
            if(seg == seg_to_free){
                trans->region->segments.erase(trans->region->segments.begin() + index);
                free(seg->mem);
                delete seg;
            }
            ++index;
        }
    }
    return;
}

void rollback(tx_t tx){
    struct transaction* trans = (struct transaction*) tx;
    //if aborting, all the locks are taken
    if (!trans->is_ro){
        //rolling back writes
        for(auto change : trans->logs){
            memcpy(change->location, change->old_data, change->size);
            free(change->old_data);
            delete change;
        }
        //rolling back free
        for(auto segment : trans->to_free){
            segment->freed = false;
        }
        //rolling back allocs
        free_segments(tx, trans->new_segments);

        for (auto lock : trans->locks) {
           lock->unlock();
        }
        for (auto lock : trans->to_free_locks) {
           lock->unlock();
        }
    }

    //unlocking
    for (auto lock : trans->read_locks) {
       lock->unlock_shared();
    }
    delete trans;
    return;
}

bool check_lock(tx_t tx, shared_mutex* lock){
    struct transaction * trans = (struct transaction*) tx;
    for(auto candidate : trans->locks){
       if (candidate == lock) {
            return true;
       }
    }
    for(auto candidate : trans->read_locks){
       if (candidate == lock) {
            return true;
       }
    }
    for(auto candidate : trans->new_seg_locks){
       if (candidate == lock) {
            return true;
       }
    }
    for(auto candidate : trans->to_free_locks){
       if (candidate == lock) {
            return true;
       }
    }
    return false;

}


//================================================================
// End of Helper functions
//================================================================

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    struct transaction* tx = new (std::nothrow) struct transaction();
    if(unlikely(tx == NULL)){
       return invalid_tx;
    }
    tx->region = (struct region*) shared;
    tx->is_ro = is_ro;
    return (tx_t) tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx) noexcept {
    struct transaction* trans = (struct transaction*) tx;
    for (auto lock : trans->read_locks) {
       lock->unlock_shared();
    }
    if (!trans ->is_ro){
        free_segments(tx, trans->to_free);

        for (auto lock : trans->locks) {
           lock->unlock();
        }
        for (auto lock : trans->new_seg_locks){
            lock->unlock();
        }
        for(auto change : trans->logs){
            free(change->old_data);
            delete change;
        }
    }
    delete trans;
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    struct region* region = (struct region*) shared;
    for(auto seg : region->segments){
        //find segment to read on
        if(source >= seg->mem && source < seg->mem + seg->size){

            if (!check_lock(tx,&seg->lock)){
                if(!seg->lock.try_lock_shared()){
                    rollback(tx);
                    return false;
                } else {
                //if locked, remember which one
                ((struct transaction*) tx)->read_locks.push_back(&seg->lock);

                }
            }
            if (seg->freed){
                rollback(tx);
                return false;
            }
            //copy the memory
            memcpy(target, source, size);
            return true;
        }
    }
    rollback(tx);
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    struct region* reg = (struct region*) shared;
    struct transaction* trans = (struct transaction*) tx;

    for(auto const& seg : reg->segments){

        //find segment to write on
        if(target >= seg->mem && target < seg->mem + seg->size){

            //mabe i have it already
            if (!check_lock(tx,&seg->lock)){
                //no, try to lock it then
                if(!seg->lock.try_lock()){
                    //didn't work, aborting
                    rollback(tx);
                    return false;
                } else {
                    //i could lock, it is new, remember it
                    trans->locks.push_back(&seg->lock);
                }
            }

            if (seg->freed){
                //should not happen, means i'm writing on a segment i freed before
                rollback(tx);
                return false;
            }

            //prepare the log
            struct log* change = new (std::nothrow) struct log();
            if (unlikely(change == NULL)){
                rollback(tx);
                return false;
            }
            change->old_data = malloc(sizeof(byte) * size);
            if (unlikely(change->old_data == NULL)){
                rollback(tx);
                return false;
            }
            memcpy(change->old_data, target, size);
            change->size = size;
            change->location = target;

            //remember the log
            trans->logs.insert(trans->logs.begin(),change);
            //copy the memory
            memcpy(target, source, size);
            return true;
        }
    }
    //if the address is not in the given region, abort the transaction
    printf("Not found for write\n");
    rollback(tx);
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size, void** target) noexcept {
    struct region* region = (struct region*) shared;

    struct segment* seg = new (std::nothrow) struct segment();
    if (unlikely(seg == NULL)) {
        return Alloc::nomem;
    }

    if (unlikely(posix_memalign((void**) &(seg->mem), region->align, size) != 0)){
        delete seg;
        return Alloc::nomem;
    }
    memset(seg->mem, 0, size);
    *target = (void *) seg->mem;
    seg->size = size;
    seg->freed = false;
    seg->lock.lock();
    ((struct transaction*) tx)->new_seg_locks.push_back(&seg->lock); 
    ((struct transaction*) tx)->new_segments.push_back(seg);

    region->segments.push_back(seg);
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    struct region* region = (struct region*) shared;
    for(auto seg : region->segments){
        //find segment to free
        if(target == seg->mem){
            //if cannot lock it, abort
            if(!seg->lock.try_lock()){
                if (!check_lock(tx,&seg->lock)){
                    rollback(tx);
                    return false;
                }
            }
            ((struct transaction*) tx)->to_free_locks.push_back(&seg->lock);
            seg->freed = true;
            return true;
        }
    }
    //if the address is not in the given region, abort the transaction
    rollback(tx);
    return false;
}
