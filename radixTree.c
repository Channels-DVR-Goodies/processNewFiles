//
// Created by paul on 3/7/23.
//

#include "processNewFiles.h"
#include "radixTree.h"


tRadixTree * newRadixTree( void )
{
    tRadixTree * tree = calloc( 1, sizeof( tRadixTree) );
    if ( tree == NULL ) return NULL;

    tree->highWater = 0;
    tree->size = 16 * 1024;
    tree->stringSpace = calloc( 1, tree->size );
    if ( tree->stringSpace == NULL ) return NULL;

    tree->nodeCount = 100;
    tree->nodeArray = calloc( tree->nodeCount, sizeof( tRadixNode ) );
    // set up the 'free chain'
    if ( tree->nodeArray == NULL )
    {
        free( (void *)tree->stringSpace );
        tree->stringSpace = NULL;
        return NULL;
    }
    else
    {
        // leave the last node as zero to mark end-of-chain
        tRadixIndex last = tree->nodeCount - 1;
        for ( tRadixIndex i = firstFreeNode; i < last; ++i )
        {
            tree->nodeArray[i].next = i + 1;
        }
        tree->nodeArray[freeIndex].next = firstFreeNode;
    }
    return tree;
}

tRadixIterator * newRadixIterator( const char * path )
{
    tRadixIterator * iterator = calloc( 1, sizeof(tRadixIterator) );

    if ( iterator != NULL )
    {
        iterator->stack = calloc( 8, sizeof(tRadixIndex) );
        if ( iterator->stack != NULL )
        {
            iterator->depth = 1;
            iterator->stack[ iterator->depth ] = iterator->tree->nodeArray[rootIndex].children;;
        }
    }

    return iterator;
}

void freeRadixInterator( tRadixIterator * iterator )
{
    if ( iterator != NULL )
    {
        if ( iterator->stack != NULL )
        {
            free( iterator->stack );
        }
        free( iterator );
    }
}
/**
 * incrementally scan the radix tree, returning the next value on each call
 * @param iterator
 * @return the next tRadixValue stored in the tree
 */
tRadixValue radixNext( tRadixIterator * iterator )
{
    if ( iterator == NULL ) return NULL;

    tRadixIndex siblings = iterator->stack[ iterator->depth ];
    if ( siblings == 0 )
    {
        // pop up one level and process the next
        // if depth drops below 1, we're done.
    }
    else
    {
        // if the current sibling has a value, that's the next value to return
        // decend to the current sibling's children (depth first traversal)
        // if the child doesn't have a value, keep decending until one is found.
    }
    return NULL;
}



/**
 *
 * @param tree
 * @param nodeIndex
 * @param depth
 */

static const char * leader = "  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .  .";

static void recursTreeDump( const tRadixTree * tree, tRadixIndex nodeIndex, int depth )
{
    if ( depth > 20 ) return;

    while ( nodeIndex != 0 )
    {
        const tRadixNode * node = &tree->nodeArray[nodeIndex];

        fprintf( stderr, "%.*s [%02u] \'", depth*3, leader, nodeIndex );
        fwrite( &tree->stringSpace[ node->start ], node->length, 1, stderr );
        fprintf( stderr, "\'\n" );

        if ( node->children != 0 )
        {
            recursTreeDump(tree, node->children, depth + 1);
        }

        nodeIndex = node->next;
    }
}

void radixTreeDump( const tRadixTree * tree )
{
    tRadixIndex nodeIndex = tree->nodeArray[rootIndex].children;
    if ( nodeIndex == 0 )
    {
        logDebug("{empty}");
    }
    else
    {
        recursTreeDump( tree, nodeIndex, 0 );
    }
}
/**
 * Given a node index, return the key that refers to it.
 * @param tree
 * @param nodeIndex The node index for the key you're requesting
 * @param key a pointer to a pointer to the key. free() the pointer when you're done.
 * @return zero if no error, and in key, a pointer to the rebuilt key. returns a negative number on error.
 */
tError radixGetKey( const tRadixTree * tree, tRadixIndex nodeIndex, tRadixKey **key )
{
    unsigned int    depth  = 0;
    tRadixLength    length = 0;    // leave space for the trailing null
    tRadixIndex *   stack;
    tRadixKey *     keyStr = NULL;

    if (nodeIndex == 0)
    {
        return -EINVAL;
    }

    // first figure out how deep the chain is (depth), and the length
    // of the key (length) i.e. the sum of the segment lengths.
    tRadixIndex index = nodeIndex;
    while ( index != 0 )
    {
        length += tree->nodeArray[ index ].length;
        depth++;
        index = tree->nodeArray[ index ].parent;
    }

    if ( depth == 0 || length == 0 )
    {
        return -EINVAL;
    }

    // allocate space to rebuild the key into (concatenate the segments)
    keyStr = malloc( (length + 1) * sizeof(tRadixKey) );
    if ( keyStr == NULL )
    {
        *key = NULL;
        return -ENOMEM;
    }

    // temporarily allocate space for a stack. A stack structure is used to avoid a recursive
    // approach, which would also work, but causes more 'churn' and adds overhead on the CPU
    // stack. Recursion can also be harder to understand for some less-experienced engineers.
    stack = malloc( depth * sizeof(tRadixIndex) );
    if ( stack == NULL )
    {
        free( keyStr );
        *key = NULL;
        return -ENOMEM;
    }

    unsigned int stackPtr = 0;
    index = nodeIndex;
    while ( index != 0 )
    {
        stack[stackPtr++] = index;
        index = tree->nodeArray[ index ].parent;
    }

    unsigned int p = 0;
    do {
        --stackPtr;
        const tRadixNode * node = &tree->nodeArray[stack[stackPtr]];
        memcpy( &keyStr[p], &tree->stringSpace[ node->start ], node->length  );
        p += node->length;
    } while ( stackPtr > 0 );

    free( stack );
    *key = keyStr;

    return 0;
}

/**
 * We're out of free nodes, so add more
 * @param tree
 * @param addCount
 * @return returns the first free node after expansion
 */
static tRadixIndex addFreeNodes(tRadixTree *tree, tRadixLength addCount)
{
    tRadixIndex result = 0;

    tRadixLength newTotal = tree->nodeCount + addCount;
    tree->nodeArray = realloc(tree->nodeArray, newTotal * sizeof(tRadixNode) );
    if ( tree->nodeArray == NULL )
    {
        /* panic! - out of memory */
    }
    else
    {
        result = tree->nodeCount;
        // zero out the new allocation, so assumptions elsewhere that nodes start out as zeroed remains true
        memset( &tree->nodeArray[result], 0, addCount * sizeof(tRadixNode) );
        for (tRadixIndex i = result; i < (newTotal - 1); ++i )
        {
            tree->nodeArray[ i ].next = i + 1;
        }
        tree->nodeArray[freeIndex].next = result;
        tree->nodeCount = newTotal;
    }
    return result;
}

static tRadixIndex nextFreeNode( tRadixTree * tree )
{
    tRadixIndex result = 0;
    if ( tree->nodeArray != NULL )
    {
        result = tree->nodeArray[freeIndex].next;
        if ( result == 0 ) {
            result = addFreeNodes( tree, tree->nodeCount > 2 );
        }
        tree->nodeArray[freeIndex].next = tree->nodeArray[result].next;

        // make sure it's 'clean' - zero all fields
        memset( &tree->nodeArray[result], 0, sizeof(tRadixNode) );
    }
    return result;
}

static void guaranteeStringSpace( tRadixTree * tree, tRadixLength length )
{
    if ( tree->highWater + length > tree->size )
    {
        // run out of string space, so enlarge it
        tRadixOffset newSize = tree->size + (tree->size >> 1);
        tree->stringSpace = realloc(tree->stringSpace, newSize);
        if (tree->stringSpace == NULL) {
            /* panic! */
        } else {
            tree->size = newSize;
        }
    }
}

tRadixIndex radixAddChild(tRadixTree * tree, tRadixIndex parentIndex, const tRadixKey * key, tRadixValue value )
{
    tRadixIndex newNodeIndex;
    // logDebug("add child \'%s\' to [%02u]", key, parentIndex);
    tRadixLength length = (tRadixLength)strlen( key );

    guaranteeStringSpace( tree, length );

    newNodeIndex = nextFreeNode( tree );
    tRadixNode * newNode = &tree->nodeArray[newNodeIndex];

    // set up the new node
    newNode->parent  = parentIndex;
    newNode->start   = tree->highWater;
    newNode->length  = length;
    newNode->value   = value;
    tree->highWater += length; // reserve the space we're about to copy into
    memcpy( &tree->stringSpace[newNode->start], key, length );

    // add this new node to the front of the parent's list of children
    // newNode->children = 0; // not needed - free nodes are always zeroed.
    newNode->next = tree->nodeArray[parentIndex].children;
    tree->nodeArray[parentIndex].children = newNodeIndex;

    return newNodeIndex;
}

// Shorten the existing node/segment to cover only the part that matched,
// and create a new child node for the trailing part that didn't match.
tError splitNode( tRadixTree *tree, tRadixIndex originalNodeIndex, tRadixLength matchLen )
{
    tRadixIndex newChildIndex = nextFreeNode(tree);

    // make the code easier to read (and help the compiler)
    tRadixNode *newChild = &tree->nodeArray[newChildIndex];
    tRadixNode *originalNode = &tree->nodeArray[originalNodeIndex];

    // ### split the segment
// child inherits the segment from the point of divergence
    newChild->start = originalNode->start + matchLen;
    // set its length to that of the unmatched part
    newChild->length = originalNode->length - matchLen;
    // shorten the length to that of the matched part (i.e. trim the tail part that didn't match)
    originalNode->length = matchLen;

    // the original node is the parent of this new node
    newChild->parent = originalNodeIndex;
    // the new child has no siblings yet. radixAddChild() will change that in a minute
    newChild->next = 0;
    // inherit the original node's children
    newChild->children = originalNode->children;
    // this new child is the now the only child of the original node (not for long)
    originalNode->children = newChildIndex;

#if 0
    logDebug("split node:  [%02u] \'%.*s\'  [%02u] \'%.*s\'",
             originalNodeIndex, originalNode->length, &tree->stringSpace[originalNode->start],
             newChildIndex, newChild->length, &tree->stringSpace[newChild->start]);
#endif

    return 0;
}

tError radixTreeAdd(tRadixTree * tree, const tRadixKey * key, tRadixValue value )
{
    tRadixIndex nodeIndex = tree->nodeArray[rootIndex].children;
    // handle adding the first child of the root - i.e. the
    // degenerate case of the tree being completely empty
    if (nodeIndex == 0 )
    {
        return radixAddChild( tree, rootIndex, key, value );
    }

    const tRadixKey * k = key;

    tRadixIndex parent = tree->nodeArray[nodeIndex].parent;
    while (nodeIndex != 0 )
    {
        tRadixOffset start  = tree->nodeArray[nodeIndex].start;
        tRadixOffset end    = start + tree->nodeArray[nodeIndex].length;
        tRadixOffset offset = start;

        while ( *k == tree->stringSpace[offset] && offset < end )
        { k++; offset++; }

        if ( offset == start )
        {
            // even the first character didn't match, so try the next sibling
            if ( tree->nodeArray[nodeIndex].next != 0 )
            {
                // parent hasn't changed
                nodeIndex = tree->nodeArray[nodeIndex].next;
                // run around the loop again
            }
            else
            {
                // we're run out of siblings, so add the remainder of the key as a child of the parent
                return radixAddChild( tree, parent, k, value );
            }
        }
        else
        {
            // something matched, was it a partial match?
            if ( offset < end )
            {
                // there was a partial match inside an existing segment, so we need
                // to split the original node/segment at the point they diverge
                splitNode(tree, nodeIndex, (tRadixLength) (offset - start));
            }

            // at this point, nodeIndex points at the last node that matched completely,
            // irrespective of whether that node needed to be split or not.

            if ( strlen(k) == 0 ) // is there any more of the key to compare?
            {
                // it is possible that the end of the key corresponds to a node
                // that was split earlier, and so does not have a value set.
                if ( tree->nodeArray[nodeIndex].value == NULL )
                {
                    tree->nodeArray[nodeIndex].value = value;
                    return 0;
                }
                else
                {
                    // this node has a value already, so this add was for a key that already existed
                    logDebug("The key \'%s\' already exists", key);
                    return -EEXIST;
                }
            }

            // we're matched so far, but we haven't reached the end of the key

            // does this node have any children to check the remainder of the key against ?
            if ( tree->nodeArray[nodeIndex].children == 0 )
            {
                // no children below this point, so add the key remainder as the first child of this node
                return radixAddChild(tree, nodeIndex, k, value );
            }
            else
            {
                // yes, there are children, so descend a level to check them
                parent = nodeIndex;
                nodeIndex = tree->nodeArray[nodeIndex].children;
            }
        }
    }

    return -EINVAL;
}

tError radixTreeFind(tRadixTree * tree, const tRadixKey * key, tRadixValue *value )
{
    tRadixIndex nodeIndex = tree->nodeArray[rootIndex].children;

    const tRadixKey * k = key;

    while ( nodeIndex != 0 )
    {
        tRadixOffset start = tree->nodeArray[nodeIndex].start;
        tRadixOffset end = start + tree->nodeArray[nodeIndex].length;
        tRadixOffset offset = start;

        if (*k != tree->stringSpace[offset])
        {
            // even the first character ddoesn't match, so try the next sibling
            if (tree->nodeArray[nodeIndex].next != 0)
            {
                // failed at the first character, so try the next sibling at this level
                nodeIndex = tree->nodeArray[nodeIndex].next;
                // run around the loop again
            } else {
                // we're run out of siblings, so there's no match
                return -ENOKEY;
            }
        } else {
            do {
                ++k;
                ++offset;
            } while ( *k == tree->stringSpace[offset] && offset < end );

            // something matched, was it a partial match?
            if ( offset < end )
            {
                // there was a partial match inside an existing segment, so the key wasn't found
                return -ENOKEY;
            }

            // at this point, nodeIndex points at the last node that matched completely
            if ( strlen(k) == 0 ) // is there any more of the key to compare?
            {
                // reached the end of the key, so we have found what we're looking for
                *value = tree->nodeArray[nodeIndex].value;
                return 0;
            }

            // we're matched so far, but we haven't reached the end of the key

            // descend one level, and start checking the children
            nodeIndex = tree->nodeArray[nodeIndex].children;
        }
    }

    return -ENOKEY;
}