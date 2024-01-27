//
// Created by paul on 3/16/23.
//

#ifndef PROCESSNEWFILES_HASHMAP_H
#define PROCESSNEWFILES_HASHMAP_H

#define HASH_BUCKET_COUNT   256

typedef struct sHashEntry {
    tHash hash;
    void * value;
} tHashEntry;

typedef struct sHashBucket {
    unsigned int count;     // count of allocated void pointers in the array
    tHashEntry * entries;   // an array of 'count' entries
 } tHashBucket;

typedef struct {
    tHashBucket * bucket[ HASH_BUCKET_COUNT ];
} tHashMap;

static inline tHashMap * newHashMap(void) { return calloc(1, sizeof(tHashMap)); }

tError freeHashMap(tHashMap * hashmap );

tError hashMapAdd( tHashMap * hashmap, tHash hash, void * value );

tError hashMapFind( tHashMap * hashmap, tHash hash, void ** value );

tError hashMapRemove( tHashMap * hashmap, tHash hash );

#endif //PROCESSNEWFILES_HASHMAP_H
