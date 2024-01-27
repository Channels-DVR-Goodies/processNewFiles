//
// Created by paul on 3/4/23.
//

#ifndef PROCESSNEWFILES_LIST_H
#define PROCESSNEWFILES_LIST_H

typedef struct sQueueEntry {
    struct sQueueEntry * next;
    struct sQueueEntry * prev;
} tListEntry;

/* Tor the root structure, next points at the first entry in the queue (or itself, if the
 * queue is empty). prev points at the last entry in the queue (or itself, if empty) */
typedef tListEntry tListRoot;

#define listForEachEntry( root, current )   for ( current = (void *)(((tListRoot *)root)->next);\
    (void *)current != (void *)root; current = (void *)(((tListEntry *)current)->next) )

tListRoot * newList(void);
void freeList( tListRoot * root );

static inline bool listAtEnd(const void * const root, const void * const current )
{
    return ( current == root );
}

static inline tListEntry * listStart( tListRoot * root )
{
    return root->next;
}

static inline tListEntry * listNext( tListEntry * entry )
{
    return entry->next;
}

static inline tListEntry * listPrev( tListEntry * entry )
{
    return entry->prev;
}

static inline bool listEntryValid( tListEntry * entry )
{
    return (entry->next != NULL && entry->prev != NULL);
}

typedef bool (*fInsertTest)( tListEntry * current, tListEntry * toInsert );

tError listInsert( tListRoot * root, tListEntry * toInsert, fInsertTest insertTest );

tError listPrepend( tListRoot * root, tListEntry * entry );
tError listAppend( tListRoot * root, tListEntry * entry );
tError listRemove( tListEntry * entry );

#endif //PROCESSNEWFILES_LIST_H
