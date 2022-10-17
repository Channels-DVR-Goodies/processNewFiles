//
// Created by paul on 10/1/22.
//

#include "processNewFiles.h"

#include <poll.h>
#include <sys/epoll.h>
#include <time.h>
#include <ftw.h>
#include <sys/inotify.h>

#include "events.h"
#include "logStuff.h"

const char * const expiredActionAsStr[] = {
    [kRescan]    = "rescan",
    [kNew]       = "new",
    [kModified]  = "modified",
    [kMoved]     = "moved"
};

/* circular dependency, so forward-declare tWatchedTree */
typedef struct nextWatchedTree tWatchedTree;

typedef struct nextNode {
    struct nextNode * expiringNext;

    tWatchedTree *  watchedTree;

    char *          path;
    tHash           pathHash;

    enum {
        kDirectory = 1, // start at 1, so 0 indicates not set
        kFile
    } type;

    tWatchID        watchID;
    tCookie         cookie;

    tExpiredAction  expiredAction;
    time_t          expires;

    UT_hash_handle  watchHandle;
    UT_hash_handle  pathHandle;
    UT_hash_handle  cookieHandle;
} tFSNode;

typedef struct nextWatchedTree {
    struct nextWatchedTree *  next;

    tFSNode * watchHashMap;   // hashmap of the watchID
    tFSNode * pathHashMap;    // hashmap of the full rootPath, hashed
    tFSNode * cookieHashMap;  // hashmap of the cookie values (as used for IN_MOVED event pairs

    tFSNode * expiringList;   // linked list ordered by ascending expiration time

    time_t nextRescan;

    struct {
        tFileDscr fd;
    } inotify;

    tDir root;
    tDir mirror;

} tWatchedTree;

static struct {
    int 	    epollfd;
    tWatchedTree *  treeList;
    tWatchedTree *  nftwWatchedTree;
} globals;

/**
 * @brief
 * @param action
 * @return
 */
const char * expiredActionToStr( tExpiredAction action )
{
    return expiredActionAsStr[ action ];
}

/**
 * @brief
 * @param buffer
 * @param remaining
 * @param separator
 * @param string
 * @return
 */
size_t appendStr( char * buffer, size_t remaining, const char * separator, const char * string )
{
    size_t len;

    if ( remaining > 0 ) {
        strncat( buffer, separator, remaining );
        len = strlen( separator );
        if ( len <= remaining ) {
            remaining -= len;
        } else {
            remaining = 0;
        }
    }

    if ( remaining > 0 ) {
        strncat( buffer, string, remaining );
        len = strlen( string );
        if ( len <= remaining ) {
            remaining -= len;
        } else {
            remaining = 0;
        }
    }

    return remaining;
}

struct {
    int mask;
    const char * label;
} iNotifyFlags[] = {
    { IN_ACCESS,       "Access" }, /* File was accessed.  */
    { IN_MODIFY,       "Modify" }, /* File was modified.  */
    { IN_ATTRIB,       "Attrib" }, /* Metadata changed.  */
    { IN_CLOSE_WRITE,  "Close_Write" }, /* Writtable file was closed.  */
    { IN_CLOSE_NOWRITE,"Close_NoWrite" }, /* Unwrittable file closed.  */
    { IN_OPEN,         "Open" }, /* file was opened.  */
    { IN_MOVED_FROM,   "Moved_From" }, /* File was moved from X.  */
    { IN_MOVED_TO,     "Moved_To" }, /* File was moved to Y.  */
    { IN_CREATE,       "Create" }, /* Subfile was created.  */
    { IN_DELETE,       "Delete" }, /* Subfile was deleted.  */
    { IN_DELETE_SELF,  "Delete_Self" }, /* Self was deleted.  */
    { IN_MOVE_SELF,    "Move_Self" }, /* Self was moved.  */
    /* Events sent by the kernel.  */
    { IN_UNMOUNT,      "Unmount" }, /* Backing fs was unmounted.  */
    { IN_Q_OVERFLOW,   "Q_Overflow" }, /* Event queued overflowed.  */
    { IN_IGNORED,      "Ignored" }, /* File was ignored.  */
    { IN_ISDIR,        "IsDir" }, /* refers to a directory */
    { 0, NULL }
};

/**
 * @brief
 * @param buffer
 * @param remaining
 * @param event
 * @return
 */
size_t inotifyEventTypeAsStr( char * buffer, size_t remaining, const struct inotify_event * event )
{
    const char * separator = "";

    buffer[ 0 ] = '\0';

    for ( int i = 0; iNotifyFlags[ i ].mask != 0; ++i ) {
        if ( event->mask & iNotifyFlags[ i ].mask ) {
            remaining = appendStr( buffer, remaining, separator, iNotifyFlags[ i ].label );
            separator = " | ";
        }
    }

    return remaining;
}

/**
 * @brief
 * @param buffer
 * @param remaining
 * @param watchedTree
 * @param fsNode
 */
void fsNodeAsStr( char * buffer, size_t remaining, const tFSNode * fsNode )
{
    /* Print the name of the watched directory. */
    if ( fsNode != NULL ) {
        char pathStr[ PATH_MAX ];

        const tWatchedTree * watchedTree = fsNode->watchedTree;
        snprintf( pathStr, sizeof( pathStr ), "%.*s{%s}",
                  (int)watchedTree->root.pathLen,
                  fsNode->path,
                  &fsNode->path[ watchedTree->root.pathLen ] );

        char cookieStr[ 32 ];
        cookieStr[ 0 ] = '\0';
        if ( fsNode->cookie != 0 ) {
            snprintf( cookieStr, sizeof( cookieStr ), " {%u}", fsNode->cookie );
        }

        char expiresStr[ 32 ];
        expiresStr[ 0 ] = '\0';
        if ( fsNode->expires != 0 ) {
            snprintf( expiresStr, sizeof( expiresStr ),
                      " %s action in %lu secs",
                      expiredActionToStr( fsNode->expiredAction ),
                      fsNode->expires - time(NULL ));
        }

        snprintf( buffer, remaining,
                  "[%02d] \'%s\'%s%s",
                  fsNode->watchID,
                  pathStr,
                  cookieStr,
                  expiresStr );
    }
}

/**
 * @brief
 * @param watchedTree
 * @param fsNode
 */
void displayFsNode( const tFSNode * fsNode )
{
    char   buffer[PATH_MAX + 128];

    size_t remaining = sizeof( buffer );
    fsNodeAsStr( buffer, remaining, fsNode );
    logDebug( "%s", buffer );
}

/**
 * @brief
 * @param watchedTree
 * @param watchedNode
 * @param event
 */
void displayEvent( const tFSNode * watchedNode, const struct inotify_event * event )
{
    if ( event != NULL ) {
        char eventTypeStr[42];

        /* display event type. */
        inotifyEventTypeAsStr( eventTypeStr, sizeof( eventTypeStr ) - 1, event );

        char cookieStr[32];
        cookieStr[ 0 ] = '\0';
        if ( event->cookie != 0 ) {
            snprintf( cookieStr, sizeof( cookieStr ), " (%u)", event->cookie );
        }

        /* the name of the file, if any. */
        const char * nameStr = "";
        if ( event->len ) {
            nameStr = event->name;
        }

        char nodeStr[PATH_MAX];
        nodeStr[ 0 ] = '\0';

        const tWatchedTree * watchedTree = watchedNode->watchedTree;
        if ( watchedTree != NULL ) {

            tFSNode * fsNode;
            HASH_FIND( watchHandle, watchedTree->watchHashMap, &event->wd, sizeof( tWatchID ), fsNode );
            if ( fsNode == NULL ) {
                logError( "Error: watchID [%02d] not found", event->wd );
            } else {
                fsNodeAsStr( nodeStr, sizeof( nodeStr ), watchedNode );
            }
        }
        logDebug( "[%02u] %-32s %-8s %-20s %s", event->wd, eventTypeStr, cookieStr, nameStr, nodeStr );
    }
}

/**
 * @brief
 * @param string
 * @return
 */
tHash calcHash( const char * string )
{
    tHash result = 0xDeadBeef;

    const char * p = string;
    while ( *p != '\0' ) {
        result = (result * 43) ^ *p++;
    }

    return result;
}

/**
 * @brief
 * @param front
 * @param back
 * @return
 */
const char * catPath( const char * front, const char * back )
{
    char * result;

    if ( front == NULL ) {
        result = NULL;
    } else if ( back == NULL || strlen( back ) == 0 ) {
        result = strdup( front );
    } else {
        size_t l = strlen( front ) + 1 + strlen( back ) + 1;
        result = calloc( l, sizeof( char ));
        if ( result != NULL ) {
            strncpy( result, front, l );
            strncat( result, "/", l );
            strncat( result, back, l );
        }
    }

    return (const char *)result;
}

/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @return
 */
const char * toRelativePath( const tWatchedTree * watchedTree, const char * fullPath )
{
    const char * result = &fullPath[ watchedTree->root.pathLen ];
    if ( *result == '/' ) {
        ++result;
    }

    return result;
}

/**
 * @brief
 * @return
 */
time_t nextExpiration( void )
{
    time_t result;

    time_t whenExpires = (unsigned)(-1L); // the end of time...
#ifdef DEBUG
    const char * whatExpires = NULL;
    tExpiredAction whyExpires  = kRescan;
#endif

    for ( tWatchedTree * watchedTree = globals.treeList; watchedTree != NULL; watchedTree = watchedTree->next ) {
        if ( watchedTree->nextRescan < whenExpires ) {
            whenExpires = watchedTree->nextRescan;
#ifdef DEBUG
            whyExpires  = kRescan;
            whatExpires = "rescan";
#endif
        }

        /* head of the list should be the next to expire */
        tFSNode const * pathNode = watchedTree->expiringList;
        if ( pathNode != NULL && pathNode->expires != 0 && whenExpires > pathNode->expires ) {
            whenExpires = pathNode->expires;
#ifdef DEBUG
            whyExpires  = pathNode->expiredAction;
            whatExpires = pathNode->path;
#endif
        }
    }

    result = whenExpires - time(NULL );
    logDebug( "%s expires %s in %ld seconds",
              whatExpires, expiredActionToStr( whyExpires ), result );
    if ( result < 0 ) {
        result = 1;
    }

    return result;
}


/**
 * @brief
 * @param newNode
 * @param existingNode
 * @return
 */
int orderedByExpiration( const tFSNode * newNode, const tFSNode * existingNode )
{
    if ( newNode->expires < existingNode->expires ) {
        return -1;
    }
    if ( newNode->expires > existingNode->expires ) {
        return 1;
    }
    return 0;
}

/**
 * @brief
 * @param watchedTree
 * @param fsNode
 * @param action
 * @param expires
 */
void resetExpiration( tFSNode * fsNode, tExpiredAction action )
{
    time_t expires = time(NULL ) + 10;

    if (fsNode != NULL)
    {
        fsNode->expiredAction = action;
        if ( fsNode->expires != expires ) {
            tWatchedTree * watchedTree = fsNode->watchedTree;
            if ( watchedTree->expiringList != NULL ) {
                LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
            }
            fsNode->expires = expires;
            if ( expires > 0 ) {
                LL_INSERT_INORDER2( watchedTree->expiringList, fsNode, orderedByExpiration, expiringNext );
            }
        }
    }
}

/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @param action
 * @param expires
 * @return
 */
tFSNode * makeFileNode( tWatchedTree * watchedTree,
                        const char * fullPath,
                        tExpiredAction action )
{
    tFSNode * fileNode = NULL;

    tHash hash = calcHash( fullPath );

    HASH_FIND( pathHandle, watchedTree->pathHashMap, &hash, sizeof( tHash ), fileNode );
    if ( fileNode == NULL ) {
        // didn't find the hash - must be a new one, so make a matching fsNode
        fileNode = calloc( 1, sizeof( tFSNode ));
        if ( fileNode != NULL ) {
            fileNode->watchedTree = watchedTree;
            fileNode->type        = kFile;
            fileNode->path        = strdup( fullPath );
            fileNode->pathHash    = calcHash( fileNode->path );
            HASH_ADD( pathHandle, watchedTree->pathHashMap, pathHash, sizeof( tHash ), fileNode );

#ifdef DEBUG
            char nodeStr[ PATH_MAX ];
            fsNodeAsStr( nodeStr, sizeof( nodeStr ), fileNode );
            logDebug( "new fileNode: %s", nodeStr );
#endif
        }
    }
    if ( fileNode != NULL ) {
        resetExpiration( fileNode, action );
    }

    return fileNode;
}

/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @return
 */
tError watchDirectory( tWatchedTree * watchedTree, const char * fullPath )
{
    tWatchID watchID = inotify_add_watch( watchedTree->inotify.fd, fullPath, IN_ALL_EVENTS);
    if ( watchID == -1 ) {
        logError( "error watching directory \'%s\'", fullPath );
        return -errno;
    } else {
        tFSNode * fsNode;
        HASH_FIND( watchHandle, watchedTree->watchHashMap, &watchID, sizeof( tWatchID ), fsNode );

        if ( fsNode == NULL ) {
            fsNode = calloc( 1, sizeof( tFSNode ));
            if ( fsNode != NULL ) {
                fsNode->watchedTree = watchedTree;
                fsNode->type        = kDirectory;
                fsNode->watchID     = watchID;
                fsNode->path        = strdup( fullPath );
                fsNode->pathHash    = calcHash( fsNode->path );
                HASH_ADD( watchHandle, watchedTree->watchHashMap, watchID, sizeof( tWatchID ), fsNode );
                HASH_ADD( pathHandle,  watchedTree->pathHashMap, pathHash, sizeof( tHash ), fsNode );
            }
            logDebug( "new dir: %s", fullPath );
        }

        if ( fsNode != NULL ) {
            displayFsNode( fsNode );
        }
    }
    return 0;
}


/**
 * @brief destroy a fsNode and remove any references to it
 * Typically this is a fileNode that expired, so it's no
 * longer of interest.
 * @see removeNode()
 * @param fsNode
 */
void forgetNode( tFSNode * fsNode )
{
    tWatchedTree * watchedTree = fsNode->watchedTree;

    if ( fsNode->watchID != 0 ) {
        inotify_rm_watch( watchedTree->inotify.fd, fsNode->watchID );
    }
    if ( watchedTree->expiringList != NULL ) {
        LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
    }
    if ( watchedTree->pathHashMap != NULL ) {
        HASH_DELETE( pathHandle, watchedTree->pathHashMap, fsNode );
    }
    free( fsNode->path );
    free( fsNode );
}

/**
 * @brief node is disappearing, so remove it from our structures, too.
 * Removes any file that may have been created in the mirror hierarchy, too.
 * @see forgetNode()
 * @param fsNode
 */
void removeNode( tFSNode * fsNode )
{
    if ( fsNode == NULL ) {
        return;
    }

    logDebug( "remove \'%s\' [%02d]", fsNode->path, fsNode->watchID  );

    tWatchedTree * watchedTree = fsNode->watchedTree;
    if ( fsNode->type == kFile ) {
        const char * relPath = toRelativePath( watchedTree, fsNode->path );
        if ( relPath != NULL ) {
            /* if a file is created and then deleted before it's ever processed, then
             * it won't be present in the .mirror hierarchy yet - and that's normal */
            if ( unlinkat( watchedTree->mirror.fd, relPath, 0 ) == -1 && errno != ENOENT ) {
                logError( "failed to delete \'%s\'", fsNode->path );
            }
            errno = 0;
        }
    }

    forgetNode( fsNode );
}

/**
 * @brief
 * @param watchedTree
 * @param watchID
 */
void ignoreWatchID( tWatchedTree * watchedTree, tWatchID watchID )
{
    /* the inotify ID has already been removed, and we won't be
     * seeing it again. so clean up our parallel structures */
    tFSNode * fsNode;

    HASH_FIND( watchHandle, watchedTree->watchHashMap, &watchID, sizeof( tWatchID ), fsNode );
    if ( fsNode != NULL ) {
        logDebug( "ignore [%d]", watchID );
        removeNode( fsNode );
    }
}

/**
 * @brief
 * @param action
 * @param path
 * @return
 */
tError queueFileToProcess( tExpiredAction action, const char * path )
{
    tError   result = 0;
    (void)action;
    (void)path;

#if 0
    tExecMsg message;

    message.action = action;
    strncpy( message.path, path, sizeof( message.path ));

    ssize_t len = write( g.pipe[ kPipeWriteFD ], &message, sizeof( tExpiredAction ) + strlen( message.path ) + 1 );
    if ( len == -1 ) {
	logError( "writing to the pipe failed" );
	result = -errno;
    }

    /* leave a persistent marker to remember this file has been mirror before when rescanning */
    const char * relPath = toRelativePath( nftwWatchedTree, fsNode->path );
    tFileDscr fd = openat( nftwWatchedTree->mirror.fd,
                           relPath,
                           O_WRONLY | O_CREAT,
                           S_IRUSR | S_IWUSR );
    if ( fd == -1 ) {
        logError( "unable to mirror %s", fsNode->path );
        result = -errno;
    } else {
        close( fd );
    }
#endif

    return result;
}

/**
 * @brief
 * @return
 */
tError processExpiredFSNodes( void )
{
    int result = 0;

    for ( tWatchedTree * watchedTree = globals.treeList;
          watchedTree != NULL;
          watchedTree = watchedTree->next ) {
        long now = time(NULL );
        for ( tFSNode * fsNode = watchedTree->expiringList;
              fsNode != NULL && fsNode->expires <= now;
              fsNode = fsNode->expiringNext ) {

            logDebug( "expired %s \'%s\'",
                      expiredActionToStr( fsNode->expiredAction ), fsNode->path );

            result = queueFileToProcess( fsNode->expiredAction, fsNode->path );
            if ( result == 0 ) {
                /* if it was queued successfully, 'forget' the fsNode,
                 * as we're done monitoring this file for now */
                forgetNode( fsNode );
            }
        }
    }
    return result;
}

/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @param pathNode
 */
void iNotifyDelete( tFSNode * pathNode, const char * fullPath  )
{
    tWatchedTree * watchedTree = pathNode->watchedTree;
    if ( pathNode->type == kFile ) {
        const char * relPath = toRelativePath( watchedTree, fullPath );

        if ( unlinkat( watchedTree->mirror.fd, relPath, 0 ) == -1 ) {
            logError( "Error: unable to delete the file in the 'mirror' shadow hierarchy\n" );
        }
    }
    removeNode( pathNode );
}

/**
 * @brief
 * @param watchedTree
 * @param event
 * @param fullPath
 * @param expires
 * @param pathNode
 */
void iNotifyCreate( tFSNode * pathNode,
                    const struct inotify_event * event,
                    const char * fullPath )
{
    tWatchedTree * watchedTree = pathNode->watchedTree;
    if ( event->mask & IN_ISDIR ) {
        watchDirectory( watchedTree, fullPath );
    } else {
        /* we already made a new fileNode, if it didn't already exist */
        resetExpiration( pathNode, kNew );
    }
}

/**
 * @brief
 * @param watchedTree
 * @param expires
 * @param pathNode
 */
void iNotifyCloseWrite( tFSNode * pathNode )
{
    tExpiredAction action = pathNode->expiredAction;
    if ( action != kNew ) {
        action = kModified;
    }
    resetExpiration( pathNode, action );
}

/**
 * @brief
 * @param watchedTree
 * @param event
 * @param fullPath
 * @param hash
 * @param expires
 * @param pathNode
 */
void iNotifyMove( tFSNode * pathNode,
                  const struct inotify_event * event,
                  const char * fullPath )
{
    tWatchedTree * watchedTree = pathNode->watchedTree;

    /* The IN_MOVED_FROM and IN_MOVED_TO are issued in pairs,
     * tied together with the same (non-zero) cookie value  */
    if ( event->cookie != 0 ) {
        tFSNode * cookieNode;
        HASH_FIND( cookieHandle, watchedTree->cookieHashMap, &event->cookie, sizeof( tCookie ), cookieNode );
        if ( cookieNode == NULL ) {
            cookieNode = pathNode;
            cookieNode->cookie = event->cookie;
            HASH_ADD( cookieHandle, watchedTree->cookieHashMap, cookie, sizeof( tCookie ), cookieNode );
        }

        // occurs first of the pair - figure out the existing pathNode
        if ( event->mask & IN_MOVED_FROM ) {
            logDebug( "{%u} move [%02d] from \'%s\' #%lu",
                      cookieNode->cookie, cookieNode->watchID, cookieNode->path, cookieNode->pathHash );
        }

        // occurs second of the pair - update the root.path in the cookieNode identified in the IN_MOVED_FROM
        if ( event->mask & IN_MOVED_TO && cookieNode != NULL ) {
            HASH_DELETE( pathHandle, watchedTree->pathHashMap, cookieNode );
            free((void *)cookieNode->path );
            cookieNode->path     = strdup( fullPath );
            cookieNode->pathHash = calcHash( cookieNode->path );
            HASH_ADD( pathHandle, watchedTree->pathHashMap, pathHash, sizeof( tHash ), cookieNode );

            HASH_DELETE( cookieHandle, watchedTree->cookieHashMap, cookieNode );
            cookieNode->cookie = 0;

            resetExpiration( cookieNode, kMoved );

            logDebug( "{%u} move [%02d] to \'%s\'",
                      cookieNode->cookie, cookieNode->watchID, cookieNode->path );
        }
    }
}

/**
 * @brief
 * @param pathNode
 * @param event
 * @param fullPath
 */
void processiNotify( tFSNode * pathNode,
                     const struct inotify_event * event,
                     const char * fullPath )
{
    if ( pathNode == NULL ) {
        return;
    }

    tWatchedTree * watchedTree = pathNode->watchedTree;
    if ( event->mask & IN_DELETE ) {
        iNotifyDelete( pathNode, fullPath );
    } else if ( event->mask & IN_CREATE ) {
        iNotifyCreate( pathNode, event, fullPath );
    } else if ( event->mask & IN_CLOSE_WRITE ) {
        iNotifyCloseWrite( pathNode );
    } else if ( event->mask & (IN_MOVED_FROM | IN_MOVED_TO)) {
        iNotifyMove( pathNode, event,  fullPath );
    } else if ( event->mask & IN_Q_OVERFLOW ) {
        /* dropped some events - rescan immediately */
        watchedTree->nextRescan = time(NULL );
    } else if ( event->mask & IN_IGNORED ) {
        /* for whatever reason, the watchID for this node will be invalid
         * after this event, so clean up any references we have to it */
        ignoreWatchID( watchedTree, event->wd );
    } else { /* all other events */
        if ( pathNode->expires != 0 ) {
            resetExpiration( pathNode, pathNode->expiredAction );
            logDebug( "deferred %s expiration of \'%s\'",
                      expiredActionToStr( pathNode->expiredAction ), pathNode->path );
        }
    }
}

/**
 * @brief
 * @param watchedTree
 * @param event
 * @return
 */
tError processInotifyEvent( tWatchedTree * watchedTree, const struct inotify_event * event )
{
    int result = 0;

    tFSNode * watchedNode;
    HASH_FIND( watchHandle, watchedTree->watchHashMap, &event->wd, sizeof( tWatchID ), watchedNode );
    if ( watchedNode == NULL ) {
        return -1;
    }

    displayEvent( watchedNode, event );
    /* Caution: if event->len is zero, the string at event->name may not be a valid C string!
     * If the event has no 'name', there's literally no string, NOT even an empty one.
     * event->name could be pointing at the beginning of the next event, or even after the
     * end of the buffer if it's the last event in it. */
    const char * fullPath = watchedNode->path;
    if ( event->len > 0 ) {
        fullPath = catPath( watchedNode->path, event->name );
    }
    tHash hash = calcHash( fullPath );

    tFSNode * pathNode;
    HASH_FIND( pathHandle, watchedTree->pathHashMap, &hash, sizeof( tHash ), pathNode );
    if ( pathNode == NULL && !(event->mask & IN_ISDIR)) {
        /* didn't find the hashed full path in this watchedTree, so create
         * a fileNode to track activity, and process it when it ceases */
        struct stat fileInfo;
        if ( stat( fullPath, &fileInfo ) == -1 && errno != ENOENT ) {
            logError( "unable to get information about \'%s\'", fullPath );
        } else {
            pathNode = makeFileNode( watchedTree, fullPath, kNew );
        }
    }

#ifdef DEBUG
    if ( pathNode != NULL && watchedNode != pathNode ) {
        char nodeStr[PATH_MAX];
        fsNodeAsStr( nodeStr, sizeof( nodeStr ), pathNode );
        logDebug( "%57c pathNode: %s", ' ', nodeStr );
    }
#endif

    processiNotify( pathNode, event, fullPath );

    if ( event->len > 0 ) {
        free( (void *)fullPath );
    }

    return result;
}

/**
 * @brief
 */
void stopLoops( void )
{
    g.running = false;
}

/**
 * @brief
 * @param epollEvent
 * @return
 */
int processEpollEvent( const struct epoll_event * epollEvent )
{
    int result = 0;
    ssize_t len;

    /* Loop over the epoll events we just read from the epoll file descriptor. */
    if ( epollEvent->events & EPOLLIN ) {
        /* Some systems cannot read integer variables if they are not properly aligned.
         * On other systems, incorrect alignment may decrease performance.
         * Hence, the buffer used for reading from the inotify.fd file descriptor
         * should have the same alignment as struct inotify_event. */

        char buf[4096]
                 __attribute__ ((aligned(__alignof__(struct inotify_event))));

        tWatchedTree * watchedTree = epollEvent->data.ptr;

        len = read( watchedTree->inotify.fd, buf, sizeof( buf ));
        /* If the nonblocking read() found no events to read, then it returns
         * -1 with errno set to EAGAIN. In that case, we exit the loop. */

        if ( len < 0 && errno != EAGAIN ) {
            logError( "unable to read from iNotify fd" );
            result = -errno;
        } else {
            /* Loop over all iNotify events in the buffer. */
            const char * event = buf;
            while ( event < buf + len ) {
                const struct inotify_event * iEvent = (const struct inotify_event *)event;
                result = processInotifyEvent( watchedTree, iEvent );
                event += sizeof( struct inotify_event ) + iEvent->len;
            }
        }
    }

    return result;
}


/**
 * @brief
 * @param fullPath
 * @param ftwbuf
 * @param watchedTree
 * @return
 */
tNFTWresult scanFileNode( tWatchedTree *     watchedTree,
                          const char *       fullPath,
                          const struct FTW * ftwbuf )
{
    if ( fullPath[ ftwbuf->base ] != '.' ) {
        const char * relPath = toRelativePath( watchedTree, fullPath );
        logDebug( "%d%-*c file: %s", ftwbuf->level, ftwbuf->level * 4, ':', relPath );

        if ( faccessat( watchedTree->mirror.fd, relPath, X_OK, 0 ) == 0 ) {
            /* recover from an interruption: shadow file is *already* present and executable */
            logDebug( "recover \'%s\'", fullPath );
            queueFileToProcess( kRescan, fullPath );
        } else {
            switch (errno) {
            case EACCES: /* shadow file is present, but not executable */
                /* we've already seen this file and processed it, nothing to do */
                break;

            case ENOENT: /* shadow file is missing, so create a file node that expires in
                            10 seconds. If it does expire, then queue it to be processed */
                makeFileNode( watchedTree, fullPath, kNew );
                break;

            default:
                logError( "Failed to get access info about shadow file \'%s\'", relPath );
                break;
            }
        }
    }

    return FTW_CONTINUE;
}

/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @param ftwbuf
 * @return
 */
tNFTWresult scanDirNode( tWatchedTree *      watchedTree,
                         const char *        fullPath,
                         const struct FTW *  ftwbuf )
{
    int result = FTW_SKIP_SUBTREE;

    if ( fullPath[ ftwbuf->base ] != '.' && strcmp( fullPath, watchedTree->mirror.path ) != 0 ) {
        result = FTW_CONTINUE;
        const char * relPath = toRelativePath( watchedTree, fullPath );
        logDebug( "%d%-*c  dir: %s",
                  ftwbuf->level, ftwbuf->level * 4, ':', relPath );
        /* make sure that the corresponding shadow directory exists */
        if ( strlen( relPath ) > 0 ) {
            if ( mkdirat( watchedTree->mirror.fd, relPath, S_IRWXU) == -1
                && errno != EEXIST ) {
                logError( "Unable to create directory %s{%s}",
                          watchedTree->mirror.path, relPath );
            }
            errno = 0;
        }

        watchDirectory( watchedTree, fullPath );
    }

    return result;
}

/**
 * @brief nftw handler for (re)scanning the tree
 * @param fullPath
 * @param sb
 * @param typeflag
 * @param ftwbuf
 * @return
 */
tNFTWresult scanNode( const char * fullPath, const struct stat * sb, int typeflag, struct FTW * ftwbuf )
{
    (void)sb;
    tNFTWresult result = FTW_CONTINUE;

    if ( globals.nftwWatchedTree != NULL ) {
        switch ( typeflag ) {
        case FTW_F: // fullPath is a regular file.
            result = scanFileNode( globals.nftwWatchedTree, fullPath, ftwbuf );
            break;

        case FTW_D: // fullPath is a directory.
            result = scanDirNode( globals.nftwWatchedTree, fullPath, ftwbuf  );
            break;

        default:
            logError( "Error: %d: unhandled typeflag %d for %s", ftwbuf->level, typeflag, fullPath );
            break;
        }
    }

    errno = 0;
    return result;
}


/**
 * @brief scan for files we have not seen before
 * @param watchedTree
 * @return
 */
tError rescanTree( tWatchedTree * watchedTree )
{
    int result = 0;

    globals.nftwWatchedTree = watchedTree;

    result = nftw( watchedTree->root.path, scanNode, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
    if ( result != 0 ) {
        logError( "Error: couldn't watch new files in the \'%s\' directory", watchedTree->root.path );
        result = -errno;
    }

    globals.nftwWatchedTree = NULL;

    watchedTree->nextRescan = time(NULL ) + 60;

    return result;
}

/**
 * @brief
 * @return
 */
tError checkRescans( void )
{
    tError result = 0;

    for ( tWatchedTree * watchedTree = globals.treeList; watchedTree != NULL; watchedTree = watchedTree->next ) {
        /* re-scan the hierarchy periodically, and when forced */
        if ( time(NULL ) >= watchedTree->nextRescan ) {
            result = rescanTree( watchedTree );
            logDebug( "next periodic in %ld secs",
                      watchedTree->nextRescan - time( NULL ));
        }
    }

    return result;
}

/**
 * @brief
 * @return
 */
tError eventLoop( void )
{
    int                result = 0;
    struct epoll_event epollEvents[32];

    g.running = true;
    do {
        time_t secsUntil = nextExpiration();

        /* * * * * * block waiting for events or a timeout * * * * * */
        errno = 0;
        logDebug("%d %p %ld %d",
                 globals.epollfd,
                 epollEvents,
                 sizeof( epollEvents ) / sizeof( struct epoll_event ),
                 (int)(secsUntil * 1000));
        int count = epoll_wait( globals.epollfd,
                                epollEvents,
                                sizeof( epollEvents ) / sizeof( struct epoll_event ),
                                (int)(secsUntil * 1000));
        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        if ( count > 0 ) {
            for ( int i = 0; i < count; ++i ) {
                result = processEpollEvent( &epollEvents[ i ] );
                if ( result != 0 )
                    break;
            }
        } else {
            switch ( errno ) {
            case EINTR:
            case EAGAIN:
                /* harmless */
                break;

            default:
                logError( "epoll() returned %d", count );
                break;
            }
        }

        if ( result == 0 ) {
            result = processExpiredFSNodes();
        }

        if ( result == 0 ) {
            result = checkRescans();
        }

    } while ( result == 0 && g.running == true );

    logInfo( "loop terminated %d", result );
    return result;
}

/**
 * @brief
 * @param watchedTree
 * @return
 */
int initEvents( tWatchedTree * watchedTree )
{
    int result = 0;
    watchedTree->inotify.fd = inotify_init();
    if ( watchedTree->inotify.fd == -1 ) {
        logError( "Unable to register for filesystem events" );
        result = -errno;
    } else {
        struct epoll_event epollEvent;
        epollEvent.events   = EPOLLIN;
        epollEvent.data.ptr = watchedTree;

        globals.epollfd = epoll_create1( 0 );

        if ( epoll_ctl( globals.epollfd, EPOLL_CTL_ADD, watchedTree->inotify.fd, &epollEvent ) == -1 ) {
            logError( "unable to register inotify.fd fd %d with epoll fd %d",
                      watchedTree->inotify.fd, globals.epollfd );
            result = -errno;
        }
    }

    if ( result != 0 ) {
        free( watchedTree );
        watchedTree = NULL;
    } else {
        watchedTree->next = globals.treeList;
        globals.treeList  = watchedTree;
    }

    return result;
}

/**
 * @brief
 * @param dir
 * @return
 */
tError createTree( const char * dir )
{
    int result = 0;

    tWatchedTree * watchedTree;

    logDebug( " processing tree \'%s\'", dir );

    watchedTree = calloc( 1, sizeof( tWatchedTree ));
    if ( watchedTree != NULL ) {
        /* since structure was calloc'd, it's unnecessary to set pointers to NULL */

 //       nftwWatchedTree->root.path = normalizePath( dir );
        if ( watchedTree->root.path == NULL ) {
            logError( "unable to normalize the root.path \'%s\'", dir );
            result = -errno;
        } else {
            logDebug( " absolute root.path \'%s\'", watchedTree->root.path );
            watchedTree->root.pathLen = strlen( watchedTree->root.path );
            watchedTree->root.fd      = open( watchedTree->root.path, O_DIRECTORY );
            if ( watchedTree->root.fd < 0 ) {
                logError( "couldn't open the \'%s\' directory", dir );
                result = -errno;
            } else {
//                result = makeMirrorDir( nftwWatchedTree );
            }
        }
        logDebug( "root.fd: %d, mirror.fd: %d", watchedTree->root.fd, watchedTree->mirror.fd );

        watchedTree->nextRescan = time(NULL ) + 60;

        if (result == 0) {
            result = initEvents( watchedTree );
        }

        if (result == 0) {
            result = rescanTree( watchedTree );
        }

        if (result == 0) {
            result = eventLoop();
        }

        if (result != 0) {
            free ( watchedTree );
            watchedTree = NULL;
        }
    }

    return result;
}

/**
 * @brief
 * @param dir
 * @return
 */
tError watchTree( const char * dir )
{
    pid_t pid = fork();
    switch (pid)
    {
    case -1:    /* failed */
        return -errno;

    case 0:     /* child */
        /* createTree contains an event loop and is not expected to return */
        return createTree( dir );

    default:    /* parent */
        return 0;
    }
}
