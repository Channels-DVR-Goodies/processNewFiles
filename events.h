//
// Created by paul on 10/1/22.
//

#ifndef PROCESSNEWFILES__EVENTS_H_
#define PROCESSNEWFILES__EVENTS_H_

typedef uint32_t    tCookie;
typedef int         tWatchID;
typedef int         tNFTWresult;

typedef enum {
    kUnmonitored = 0, kRescan, kFirstSeen, kModified, kMoved, kRetry
} tExpiredReason;

typedef struct {
    const char *  path;
    size_t    	  pathLen;
    tFileDscr 	  fd;
} tDir;

typedef enum {
    kUnset = 0,
    kTree,      // start at 1, reserve 0 to mean 'not set'
    kDirectory,
    kFile
} tFSNodeType;

/* circular dependency, so forward-declare tWatchedTree */
typedef struct nextWatchedTree tWatchedTree;

typedef struct nextNode {
    struct nextNode * next;

    tWatchedTree *  watchedTree;

    const char *    path;
    const char *    relPath;        /* points within string pointed at 'path' */

    UT_hash_handle  pathHandle;     /* key for the path hash hashmap */
    UT_hash_handle  watchHandle;    /* key for the watchID hashmap */
    UT_hash_handle  cookieHandle;   /* key for the cookie hashmap (only used
                                     * to match up pairs of 'move' events */

    tHash           pathHash;       /* hash of the full path */
    tWatchID        watchID;        /* the watchID iNotify gave us */
    tCookie         cookie;         /* only used for the 'move' events */

    struct {
        time_t          at;             /* when it has been idle long enough (i.e. resetExpiration hasn't been called */
        tExpiredReason  because;        /* why the file was being watched in the first place */
        time_t          wait;           /* keep track of how long to wait between retries */
        int             retries;        /* keep track of how many times we've tried & failed to process this */
    } expires;

    tFSNodeType     type;
} tFSNode;

typedef struct nextWatchedTree {
    struct nextWatchedTree *  next;

    tFSNode * watchHashMap;   // hashmap of the watchID
    tFSNode * pathHashMap;    // hashmap of the full rootPath, hashed
    tFSNode * cookieHashMap;  // hashmap of the cookie values (as used for IN_MOVED event pairs

//    time_t nextRescan;

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

// tFSNode *  watchNode( tWatchedTree * watchedTree, const char * fullPath, tFSNodeType type );

pid_t   getDaemonPID( void );

tError  initDaemon( void );
tError  startDaemon( void );
void    stopDaemon( void );

#endif //PROCESSNEWFILES__EVENTS_H_
