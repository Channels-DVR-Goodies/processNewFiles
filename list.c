//
// Created by paul on 3/4/23.
//

#include "processNewFiles.h"
#include "list.h"

tListRoot * newList(void)
{
    tListRoot * result = calloc(1, sizeof(tListRoot));
    if ( result != NULL )
    {
        /* intialise to an empty circular doubly-linked list */
        result->next = result;
        result->prev = result;
    }
    return result;
}

void freeList( tListRoot * root )
{
    tListEntry * current = root->next;
    if ( current != root ) {
        root->next = root;
        root->prev = root;
        while ( current != root )
        {
            tListEntry * next = current->next;
            free( current );
            current = next;
        }
    }
}

tError listPrepend(tListRoot * root, tListEntry * entry )
{
    tListEntry * first = root->next;
    /* forwward direction */
    entry->next = first;
    root->next = entry;
    /* reverse directory */
    entry->prev = root;
    first->prev = entry;

    return 0;
}

tError listAppend(tListRoot * root, tListEntry * entry )
{
    if ( entry == NULL) return -EINVAL;

    tListEntry * last = root->prev;
    /* forward direction */
    entry->next = root;
    last->next = entry;
    /* reverse direction */
    entry->prev = last;
    root->prev = entry;

    return 0;
}

tError listRemove(tListEntry * entry )
{
    if ( entry == NULL ) {
        return -EINVAL;
    }

    tListEntry * prev = entry->prev;
    tListEntry * next = entry->next;

    /* just in case someone tries to remove the root of an empty queue */
    if ( next != entry && prev != entry )
    {
        /* unlink the entry by updating links on either side to point to each other */
        prev->next = next;
        next->prev = prev;
    }

    /* zero the links in the entry, as it's no longer part of the list */
    entry->next = entry;
    entry->prev = entry;

    return 0;
}

/**
 *
 * @param root
 * @param toInsert
 * @param insertTest
 * @return
 */
tError listInsert(tListRoot * root, tListEntry * toInsert, fInsertTest insertTest )
{
    if ( toInsert == NULL ) {
        return -EINVAL;
    }
    tListEntry * current = root->next;
    listForEachEntry( root, current )
    {
        if ( (*insertTest)( current, toInsert ) )
        {
            /* insert the new queue entry before the current one. */
            tListEntry * prev = current->prev;

            /* update the forward links */
            toInsert->next = current;
            prev->next = toInsert;
            /* update the backward links */
            toInsert->prev = prev;
            current->prev = toInsert;

            return 0;
        }
    }
    // only reach here if the entry should be appended to the list
    listAppend( root, toInsert );
    return 0;
}


