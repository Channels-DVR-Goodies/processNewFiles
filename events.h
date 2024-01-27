//
// Created by paul on 10/1/22.
//

#ifndef PROCESSNEWFILES__EVENTS_H_
#define PROCESSNEWFILES__EVENTS_H_

#include <stdint.h>
#include "list.h"

typedef uint32_t    tCookie;
typedef int         tWatchID;
typedef int         tNFTWresult;

typedef enum {
    kUnset = 0, kTreeRoot, kRescan, kFirstSeen, kModified, kMoved, kRetry
} tExpiredReason;

typedef struct {
    const char *  path;
    size_t    	  pathLen;
    tFileDscr 	  fd;
} tDir;

typedef enum {
    kTree = 1,      // start at 1, reserve 0 to mean 'not set'
    kDirectory,
    kFile
} tFSNodeType;

/* circular dependency, so forward-declare tWatchedTree */
typedef struct nextWatchedTree tWatchedTree;

typedef struct sFSNode {
    tListEntry      queue;

    tWatchedTree *  watchedTree;

    const char *    path;
    const char *    relPath;    // points within string pointed at 'path'

    tHash           pathHash;   // hash of the full path
    tWatchID        watchID;    // the watchID iNotify gave us
    tCookie         cookie;     // only used for the 'move' events

    struct {
        time_t          at;         // when it has been idle long enough (i.e. resetExpiration hasn't been called)
        tExpiredReason  because;    // why the file was being watched in the first place
        time_t          every;      // keep track of how long to wait between retries
        int             retries;    // keep track of how many times we've tried & failed to process this
    } expires;

    tFSNodeType     type;
} tFSNode;

typedef struct nextWatchedTree {
    tListEntry  queue;

    tFSNode *   rootNode;   // the tree's root node. it expires regularly to trigger a rescan of this hierarchy

    tHashMap *  pathMap;    // the path hash hashmap
    tHashMap *  watchMap;   // the watchID hashmap
    tHashMap *  cookieMap;  // the cookie hashmap (only used to match up pairs of 'move' events)

    struct {
        tFileDscr   fd;
    }  inotify;

    tDir root;
    tDir shadow;

    const char * exec;

} tWatchedTree;


void    forgetNode( tFSNode * fsNode );

tError  createTree( const char * dir, const char * exec );

tError  fileExpired( tFSNode * node );

tError  registerFdToEpoll( tFileDscr fd, uint64_t data );

pid_t   getDaemonPID( void );

tError  initDaemon( void );
tError  startDaemon( void );
void    stopDaemon( void );

#endif //PROCESSNEWFILES__EVENTS_H_
