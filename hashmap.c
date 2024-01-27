//
// Created by paul on 3/16/23.
//

#include "processNewFiles.h"

unsigned short hashToBucketIndex( tHash hash )
{
    unsigned short index = 0;
    tHash h = hash;
    while ( h > 0 )
    {
        index ^= ( h & 0xFF );
        h = h >> 8;
    }
    return index;
}

tError freeHashMap(tHashMap * hashmap )
{
    if ( hashmap == NULL ) return -EINVAL;

    for (unsigned int i = 0; i < HASH_BUCKET_COUNT; i++ )
    {
        if ( hashmap->bucket[i] != NULL )
        {
            free( hashmap->bucket[i] );
            hashmap->bucket[i] = NULL;
        }
    }
    return 0;
}

tError hashMapAdd(tHashMap * hashmap, tHash hash, void * value )
{
    unsigned short index = hashToBucketIndex( hash );
    tHashBucket * bucket = hashmap->bucket[index];
    if ( bucket == NULL )
    {
        unsigned int count = 8;
        bucket = calloc( 1, sizeof(tHashBucket) + (count * sizeof(tHashEntry)) );
        if ( bucket != NULL )
        {
            bucket->count = count;
            hashmap->bucket[index] = bucket;
        }
    }
    if ( bucket == NULL ) return -ENOMEM;

    for ( unsigned int i = 0; i < bucket->count; ++i )
    {
        if ( bucket->entries[i].value == NULL )
        {
            bucket->entries[i].hash  = hash;
            bucket->entries[i].value = value;
            return 0;
        }
        if ( i+1 >= bucket->count )
        {
            const int increment = 8;
            // we ran out of slots, so enlarge the bucket
            bucket = realloc( bucket, sizeof(tHashBucket) + ((bucket->count + increment) * sizeof(tHashEntry)) );
            if ( bucket == NULL )
            {
                // the realloc() failed
                return -ENOMEM;
            }

            // realloc() does not clear the additional bytes
            memset( &bucket->entries[bucket->count], 0, increment * sizeof(tHashEntry) );
            bucket->count += increment;
            hashmap->bucket[index] = bucket;
        }
    }

    return 0;
}

tError hashMapFind(tHashMap * hashmap, tHash hash, void ** value )
{
    unsigned short index = hashToBucketIndex( hash );
    tHashBucket * bucket = hashmap->bucket[index];
    if ( bucket != NULL ) {
        for (unsigned int i = 0; i < bucket->count; ++i) {
            if (bucket->entries[i].hash == hash) {
                *value = bucket->entries[i].value;
                return 0;
            }
        }
    }
    return -ENOENT;
}

tError hashMapRemove(tHashMap * hashmap, tHash hash )
{
    unsigned short index = hashToBucketIndex( hash );
    tHashBucket * bucket = hashmap->bucket[index];
    if ( bucket != NULL ) {
        for (unsigned int i = 0; i < bucket->count; ++i) {
            if (bucket->entries[i].hash == hash) {
                while ( i < (bucket->count - 1) ) {
                    bucket->entries[i].hash  = bucket->entries[i+1].hash;
                    bucket->entries[i].value = bucket->entries[i+1].value;
                    ++i;
                }
                bucket->entries[i].value = 0;
                bucket->entries[i].value = NULL;
                return 0;
            }
        }
    }
    return -ENOENT;}
