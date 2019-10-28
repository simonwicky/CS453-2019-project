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
    /shared_mutex lock;
    byte* mem;
    //struct segment* previous;
    //struct segment* next;
    size_t size;
};

struct region {
    void* start;
    size_t nb_segments;
    //struct segment* first_segment;
    vector<struct segment*> segments;
    size_t size;
    size_t align;
};

struct log{
    void* old_data;
    size_t size;
    void* location;
};

struct transaction {
    vector<struct log*> logs;
    struct region* region;
    bool is_ro;
    vector<shared_mutex*> locks;
};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept{
    printf("tm_create HERE\n");
    struct region* region = (struct region*) malloc(sizeof(struct region));
    if (unlikely(region == NULL)) {
        return invalid_shared;
    }

    struct segment* seg = (struct segment*) malloc(sizeof(struct segment));
    if (unlikely(seg == NULL)) {
        free(region);
        return invalid_shared;
    }

    if (unlikely(posix_memalign(&(region->start), align, size) != 0)){
        free(seg);
        free(region);
        return invalid_shared;
    }

    memset(region->start, 0, size);
    //region->segments = {};
    region->segments.push_back(seg);
    seg->mem = (byte*) region->start;
    seg->size = size;
    //seg->lock = shared_mutex();
    printf("tm_create END HERE\n");
    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    // struct region* region = (struct region*) shared;
    // for (auto seg : region->segments){
    //     //free(seg->mem);
    //     //free(seg);
    // }
    // //free(shared);
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
void rollback(shared_t shared as(unused), tx_t tx as(unused)){
    struct transaction* trans = (struct transaction*) tx;
    //if aborting, all the locks are taken
    for(auto change : trans->logs){
        memcpy(change->location, change->old_data, change->size);
        free(change->old_data);
        free(change);
    }
    for (auto lock : trans->locks) {
        lock->unlock();
    }

    return;
}

bool check_lock(tx_t tx, shared_mutex* lock){
    struct transaction * trans = (struct transaction*) tx;
    for(auto candidate : trans->locks){
        if (candidate == lock) {
            return true;
        }
    }
    return false;

}
//================================================================
//Helper functions
//================================================================

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    //printf("TM_BEGIN: begin\n");
    struct transaction* tx = (struct transaction*) malloc(sizeof(struct transaction));
    if(unlikely(tx == NULL)){
       return invalid_tx;
    }
    tx->region = (struct region*) shared;
    tx->is_ro = is_ro;
    //printf("TM_BEGIN: end\n");
    return (tx_t) tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx) noexcept {
    printf("tm_end : HERE\n");
    // struct transaction* trans = (struct transaction*) tx;
    // for (auto lock : trans->locks) {
    //     lock->unlock();
    // }
    // if (!trans ->is_ro){
    //     for(auto change : trans->logs){
    //         //free(change->old_data);
    //         //free(change);
    //     }
    // }
    // //free(trans);
    return false;
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
    printf("tm_read HERE\n");
    // struct region* region = (struct region*) shared;

    // for(auto seg : region->segments){

    //     //find segment to read on
    //     if(target < seg->mem + size){
    //         //if cannot lock it, abort
    //         if(!seg->lock.try_lock_shared()){
    //             if (!check_lock(tx,&seg->lock)){
    //                 rollback(shared,tx);
    //                 return false;
    //             }
    //         }
    //         //if locked, remember which one
    //         ((struct transaction*) tx)->locks.push_back(&seg->lock);
    //         //copy the memory
    //         memcpy(target, source, size);
    //         return true;
    //     }
    // }
    // rollback(shared,tx);
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
    printf("tm_write HERE\n");
    struct region* reg = (struct region*) shared;
    struct transaction* trans = (struct transaction*) tx;

    for(auto const& seg : reg->segments){
    printf("tm_write LOOP HERE\n");
    //     //find segment to write on

        std::cout << seg;
        printf("tm_write BUG HERE\n");  
        if(target < seg->mem + size){
    //         //if cannot lock it, abort
            printf("tm_write FOUND SEG\n");
    //         if(!seg->lock.try_lock()){
    //             if (!check_lock(tx,&seg->lock)){
    //                 rollback(shared,tx);
    //                 printf("tm_write END HERE\n");
    //                 return false;
    //             }
    //         }
    //         //if locked, remember which one
    //         trans->locks.push_back(&seg->lock);
    //         //prepare the log
    //         struct log* change = (struct log*) malloc(sizeof(struct log));
    //         if (unlikely(change == NULL)){
    //             rollback(shared,tx);
    //             printf("tm_write END HERE\n");
    //             return false;
    //         }
    //         change->old_data = malloc(sizeof(byte) * size);
    //         if (unlikely(change->old_data == NULL)){
    //             rollback(shared,tx);
    //             printf("tm_write END HERE\n");
    //             return false;
    //         }
    //         memcpy(change->old_data, target, size);
    //         change->size = size;
    //         change->location = target;
    //         //remember the log
    //         trans->logs.insert(trans->logs.begin(),change);
    //         //copy the memory
    //         memcpy(target, source, size);
    //         printf("tm_write END HERE\n");
    //         return true;
        }
    }
    // //if the address is not in the given region, abort the transaction
    // rollback(shared,tx);
    printf("tm_write END HERE\n");
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size, void** target) noexcept {
    printf("tm_alloc HERE\n");
    // struct region* region = (struct region*) shared;

    // struct segment* seg = (struct segment*) malloc(sizeof(struct segment));
    // if (unlikely(seg == NULL)) {
    //     return Alloc::nomem;
    // }

    // if (unlikely(posix_memalign((void**) &(seg->mem), region->align, size) != 0)){
    //     //free(seg);
    //     return Alloc::nomem;
    // }
    // memset(seg->mem, 0, size);
    // *target = (void *) seg->mem;
    // region->segments.push_back(seg);
    // seg->size = size;

    return Alloc::abort;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    printf("tm_free HERE\n");
    // struct region* region = (struct region*) shared;

    // for(auto seg : region->segments){

    //     //find segment to write on
    //     if(target == seg->mem){
    //         //if cannot lock it, abort
    //         if(!seg->lock.try_lock()){
    //             if (!tm_check_lock(tx,&seg->lock)){
    //                 tm_rollback(shared,tx);
    //                 return false;
    //             }
    //         }
    //         //free(seg->mem);
    //         //free(seg);

    //         return true;
    //     }
    // }
    // //if the address is not in the given region, abort the transaction
    // tm_rollback(shared,tx);
    return false;
}
