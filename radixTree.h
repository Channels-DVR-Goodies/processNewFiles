//
// Created by paul on 3/7/23.
//

#ifndef PROCESSNEWFILES_RADIXTREE_H
#define PROCESSNEWFILES_RADIXTREE_H

typedef unsigned long   tRadixOffset;   // from the start of stringSpace
typedef unsigned short  tRadixLength;
typedef unsigned short  tRadixIndex;    // index into nodeArray
typedef char            tRadixKey;      // a C-style string
typedef void *          tRadixValue;    // an arbitrary pointer to some data structure

typedef struct sNode {
    tRadixIndex     parent;     // zero indicates top-of-tree
    tRadixIndex     next;       // linked list of siblings
    tRadixIndex     children;   // linked list of children

    tRadixOffset    start;      // start of string segment
    tRadixLength    length;     // string segment length

    tRadixValue     value;      // the value if this is an exact tail match
} tRadixNode;

enum tConstRadixIndex {
    freeIndex     = 0,
    rootIndex     = 1,
    firstFreeNode = 2
};

typedef struct {
    tRadixKey *     stringSpace;
    tRadixOffset    size;
    tRadixOffset    highWater;

    tRadixLength    nodeCount;
    tRadixNode *    nodeArray;

} tRadixTree;

typedef struct {
    tRadixTree *    tree;
    tRadixLength    depth;
    tRadixIndex *   stack;
} tRadixIterator;

tRadixTree * newRadixTree(void);
void   freeRadixTree( tRadixTree * tree );
tError radixTreeAdd(  tRadixTree * tree, const tRadixKey * key, tRadixValue value );
tError radixTreeFind( tRadixTree * tree, const tRadixKey * key, tRadixValue *value );

tRadixIterator * newRadixIterator( const char * path );
void freeRadixInterator( tRadixIterator * iterator );
tRadixValue radixNext( tRadixIterator * iterator );

void radixTreeDump( const tRadixTree * tree );

#endif //PROCESSNEWFILES_RADIXTREE_H
