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
#include "rescan.h"

typedef enum {
    kExitLoop = 1,  /* external trigger to exit the loop */
    kExecTrigger,   /* execute the next entry in the exec queue */
} tEpollSpecialValue;

typedef enum {
    kReadEnd  = 0,
    kWriteEnd = 1
} tPipeEnd;

/* to be used in messages of the form 'because it %s' */
const char * const expiredReasonAsStr[] = {
    [kUnmonitored] = "is not monitored",
    [kFirstSeen]   = "is new",
    [kModified]    = "has been modified",
    [kMoved]       = "has moved",
    [kRetry]       = "is being retried"
};

static struct {
    struct {
        tFileDscr   fd;
    } epoll;

    struct {
        tFileDscr exitLoop[2];
        tFileDscr execTrigger[2];
    } pipe;

    tWatchedTree *  treeList;
} gEvent;

const char * fsTypeAsStr[] = {
    [kFile]      = "file",
    [kDirectory] = "directory"
};


struct {
    int mask;
    const char * label;
} iNotifyFlags[] = {
    { IN_ACCESS,       "Access" },        /* File was accessed.  */
    { IN_MODIFY,       "Modify" },        /* File was modified.  */
    { IN_ATTRIB,       "Attrib" },        /* Metadata changed.  */
    { IN_CLOSE_WRITE,  "Close_Write" },   /* File opened for writing was closed.  */
    { IN_CLOSE_NOWRITE,"Close_NoWrite" }, /* File opened for reading was closed.  */
    { IN_OPEN,         "Open" },          /* File was opened.  */
    { IN_MOVED_FROM,   "Moved_From" },    /* File was moved from <src>.  */
    { IN_MOVED_TO,     "Moved_To" },      /* File was moved to <dest>.  */
    { IN_CREATE,       "Create" },        /* File was created in dir.  */
    { IN_DELETE,       "Delete" },        /* File was deleted from dir.  */
    { IN_DELETE_SELF,  "Delete_Self" },   /* Self was deleted.  */
    { IN_MOVE_SELF,    "Move_Self" },     /* Self was moved.  */
    /* Flags set by kernel */
    { IN_UNMOUNT,      "Unmount" },       /* Backing fs was unmounted.  */
    { IN_Q_OVERFLOW,   "Q_Overflow" },    /* Event queued overflowed.  */
    { IN_IGNORED,      "Ignored" },       /* File was ignored.  */
    { IN_ISDIR,        "IsDir" },         /* Event refers to a directory */
    /* end of table */
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
    buffer[ 0 ] = '\0';

    bool needSeparator = false;
    for ( int i = 0; iNotifyFlags[ i ].mask != 0; ++i ) {
        if ( event->mask & iNotifyFlags[ i ].mask ) {
            if ( needSeparator ) {
                strncat( buffer, " | ", remaining );
            }
            strncat( buffer, iNotifyFlags[ i ].label, remaining );
            needSeparator = true;
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
    /* Print the name of the watched node */
    if ( fsNode != NULL )
    {
        char cookieStr[ 32 ];
        cookieStr[ 0 ] = '\0';
        if ( fsNode->cookie != 0 ) {
            snprintf( cookieStr, sizeof( cookieStr ), " {%u}", fsNode->cookie );
        }

        char expiresStr[ 32 ];
        expiresStr[ 0 ] = '\0';
        if ( fsNode->expires != 0 )
        {
            snprintf( expiresStr, sizeof( expiresStr ),
                      " expires in %lu secs",
                      fsNode->expires - time(NULL ) );
        }

        snprintf( buffer, remaining,
                  "%s [%02d] \'%s\'%s %s%s",
                  fsTypeAsStr[ fsNode->type ],
                  fsNode->watchID,
                  fsNode->path,
                  cookieStr,
                  expiredReasonAsStr[ fsNode->expiredReason ],
                  expiresStr );
    }
}


/**
 * @brief
 * @param watchedTree
 * @param fsNode
 */
void logFsNode( const tFSNode * fsNode )
{
    char   buffer[PATH_MAX + 128];

    size_t remaining = sizeof( buffer );
    fsNodeAsStr( buffer, remaining, fsNode );
    logDebug( "%s", buffer );
}


/**
 * @brief
 * @param event
 * @param watchedNode
 * @param pathNode
 */
void logEvent( const struct inotify_event * event, const tFSNode * watchedNode )
{
#ifdef DEBUG
    /* don't log directory events. it's just extra noise at this point. */
    if ( event != NULL && !(event->mask & IN_ISDIR) )
    {
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
        if ( event->len > 0 ) {
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
        logDebug( "[%02u] %-32s %-8s %s (%s)", event->wd, eventTypeStr, cookieStr, nodeStr, nameStr );
    }
#endif
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
 * @param watchedTree
 * @param fullPath
 * @return
 */
tFSNode * fsNodeFromPath( const tWatchedTree * watchedTree, const char * fullPath )
{
    tFSNode * fsNode;

    tHash hash = calcHash( fullPath );
    HASH_FIND( pathHandle, watchedTree->pathHashMap, &hash, sizeof( tHash ), fsNode );
    return fsNode;
}


/**
 * @brief
 * @param watchedTree
 * @param watchID
 * @return
 */
tFSNode * fsNodeFromWatchID( const tWatchedTree * watchedTree, tWatchID watchID )
{
    tFSNode * fsNode;

    HASH_FIND( watchHandle, watchedTree->watchHashMap, &watchID, sizeof( tWatchID ), fsNode );
    return fsNode;
}


/**
 * @brief figure out when the next soonest expiration will occur
 * @return time_t of the next expiration
 */
time_t nextExpiration( void )
{
    time_t whenExpires = (unsigned)(-1L); // the end of time...
#ifdef DEBUG
    const char * whatExpires = NULL;
    tExpiredReason whyExpires = kUnmonitored;
#endif

    for ( tWatchedTree * watchedTree = gEvent.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        if ( watchedTree->nextRescan < whenExpires ) {
            whenExpires = watchedTree->nextRescan;
#ifdef DEBUG
            whyExpires  = kUnmonitored;
            whatExpires = NULL;
#endif
        }

#ifdef DEBUG
        for( tFSNode * node = watchedTree->expiringList; node != NULL; node = node->expiringNext ) {
            logDebug("%p: %s in %ld secs", node, node->relPath, node->expires - time( NULL ) );
        }
#endif
        /* list is sorted ascending by expiration, so the head of the list is the next to expire */
        tFSNode const * pathNode = watchedTree->expiringList;
        if ( pathNode != NULL && pathNode->expires != 0 && whenExpires > pathNode->expires ) {
            whenExpires = pathNode->expires;
#ifdef DEBUG
            whyExpires  = pathNode->expiredReason;
            whatExpires = pathNode->relPath;
#endif
        }
    }

    time_t now = time( NULL );
    if ( whenExpires < now ) {
        whenExpires = now + 1;
    } else if ( whenExpires - now > 3600 ) {
        whenExpires = now + 3600;
    }

#ifdef DEBUG
    if ( whyExpires == kUnmonitored ) {
        logDebug( "rescan in %ld seconds", whenExpires - now );
    } else {
        logDebug( "%s %s, and will expire in %ld seconds",
                  whatExpires, expiredReasonAsStr[ whyExpires ], whenExpires - now );
    }
#endif

    return whenExpires;
}


/**
 * @brief function to ensure expiringList is in ascending order of expiration
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
 * @param reason
 * @param expires
 */
void resetExpiration( tFSNode * fsNode, tExpiredReason reason )
{
    if (fsNode != NULL)
    {
        time_t expires = time(NULL ) + fsNode->idlePeriod;

        logDebug( "resetExpiration of %s", fsNode->relPath );
        fsNode->expiredReason = reason;
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
 * @param type
 * @return
 */
tFSNode * watchNode( tWatchedTree * watchedTree, const char * fullPath, tFSNodeType type )
{
    errno = 0;
    /* look up the FSNode using the hash of the full path */
    tFSNode * fsNode = fsNodeFromPath( watchedTree, fullPath );

    if ( fsNode == NULL ) {
        fsNode = calloc( 1, sizeof( tFSNode ));
        fsNode->watchedTree = watchedTree;
        fsNode->type        = type;
        fsNode->path        = strdup( fullPath );
        fsNode->pathHash    = calcHash( fsNode->path );
        fsNode->relPath     = &fsNode->path[ watchedTree->root.pathLen ];
        fsNode->idlePeriod  = g.timeout.idle;

        HASH_ADD( pathHandle, watchedTree->pathHashMap, pathHash, sizeof( tHash ), fsNode );

        switch (type)
        {
        case kFile:
            resetExpiration( fsNode, kFirstSeen );
            break;

        case kDirectory:
            fsNode->watchID = inotify_add_watch( watchedTree->inotify.fd, fullPath, IN_ALL_EVENTS);
            if ( fsNode->watchID == -1 ) {
                logError( "problem watching directory \'%s\'", fullPath );
            } else {
                HASH_ADD( watchHandle, watchedTree->watchHashMap, watchID, sizeof( tWatchID ), fsNode );
            }
            break;
        }

//        logFsNode( fsNode );
    }

    return fsNode;
}

void freeNode( tFSNode * fsNode )
{
    if (fsNode != NULL ) {
        if ( fsNode->path != NULL ) {
            free( (void *) fsNode->path );
        }
        free( fsNode );
    }
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
    if (fsNode == NULL) return;

    tWatchedTree * watchedTree = fsNode->watchedTree;

    if (watchedTree != NULL) {
        if ( fsNode->watchID != 0 ) {
            inotify_rm_watch( watchedTree->inotify.fd, fsNode->watchID );
        }
        if ( watchedTree->expiringList != NULL) {
            LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
        }
        if ( watchedTree->pathHashMap != NULL) {
            HASH_DELETE( pathHandle, watchedTree->pathHashMap, fsNode );
        }
    }
}


/**
 * @brief node is disappearing, so remove it from our structures, too.
 * Removes any file that may have been created in the shadow hierarchy, too.
 * @see forgetNode()
 * @param fsNode
 */
void removeNode( tFSNode * fsNode )
{
    if ( fsNode == NULL ) {
        return;
    }

    logDebug( "remove \'%s\' [%02d]", fsNode->path, fsNode->watchID  );

    const tWatchedTree * const watchedTree = fsNode->watchedTree;
    if ( fsNode->type == kFile ) {
        /* if a file is created and then deleted before it's ever processed, then
         * it won't be present in the shadow hierarchy yet - and that's normal */
        if ( unlinkat( watchedTree->shadow.fd, fsNode->relPath, 0 ) == -1 && errno != ENOENT ) {
            logError( "failed to delete \'%s\'", fsNode->path );
        }
        errno = 0;
    }

    forgetNode( fsNode );
}

tError retryFileNode( tFSNode * fileNode )
{
    tError result = 1;

    fileNode->retries++;        /* count the failures, so we don't retry forever */
    fileNode->idlePeriod *= 2;  /* double the idle delay each time it fails */
    /* spread the expirations out over time, to spread the load if many of them
     * fail quickly and would otherwise be retried at almost the same time. This
     * is most likely to happen if the 'exec' statement provided by the user is
     * faulty in some way, e.g. doesn't have sufficient permissions */
    // fileNode->idlePeriod += (random() % g.timeout.idle);

    if ( fileNode->retries >= 5 ) {
        logError( "failed to process \'%s\' successfully after %d retries",
                  fileNode->path,
                  fileNode->retries );
        result = -ENOTRECOVERABLE;
    } else {
        logError( "attempt %d to process \'%s\' failed, retry in %ld secs",
                  fileNode->retries,
                  fileNode->relPath,
                  fileNode->idlePeriod );
        /* put it back onto the expire list to try again */
        resetExpiration( fileNode, kRetry );
    }
    return result;
}

void markFileComplete( const tFSNode * fileNode )
{
    const tWatchedTree * watchedTree = fileNode->watchedTree;

    int fd = openat( watchedTree->shadow.fd,
                 fileNode->relPath,
                     O_RDONLY | O_CREAT | O_TRUNC,
                     S_IRUSR | S_IRGRP );
    /* openat does not apply the permissions if the
     * file isn't new, so also do it explicitly */
    fchmod( fd, S_IRUSR | S_IRGRP );
    close( fd );
    /* ToDo: free the fileNode */
}

/**
 * @brief execute the processing for the provided fileNode
 * if it fails, it will be added to the expiring list with a new
 * expiration time. Otherwise, it'll keep getting retried immediately,
 * and everything else will back up behind it
 * @param fileNode
 * @return
 */
tError execFileNode( tFSNode * fileNode )
{
    tError   result = 0;

    logDebug( "### execute %s", fileNode->path );

    /* ToDo: Actually execute it */
    result = -1;

    if (result == 0) {
        /* processing completed without error, so adjust the shadow file accordingly */
        markFileComplete( fileNode );
    } else {
        result = retryFileNode( fileNode );
        if ( result == 0 || result == -ENOTRECOVERABLE ) {
            /* otherwise it'll never end... */
            markFileComplete( fileNode );
        }
    }
    return result;
}


/**
 * @brief
 * @param fileNode
 * @return
 */
tError fileExpired( tFSNode * fileNode )
{
    tError   result = 0;
    const tWatchedTree * watchedTree = fileNode->watchedTree;

    logDebug( "\'%s\' %s, and has expired", fileNode->path, expiredReasonAsStr[ fileNode->expiredReason ] );

    int fd = openat( watchedTree->shadow.fd, fileNode->relPath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU );
    if ( fd == -1 ) {
        result = -errno;
        logError( "unable to create shadow file \'%s%s\'", watchedTree->shadow.path, fileNode->relPath );
    } else {
        char * buffer;
        /* If the shadow file already exists, e.g. a file that was previously
         * processed and thus is already R/O, and is then written to again,
         * then the openat() above won't have affected the shadow file's perms.
         * So also do it explicitly. */
        fchmod( fd, S_IRWXU );

        asprintf( &buffer, "#!/bin/bash\nFILE=\'%s\'\nREASON=\'%s\'\n%s\n",
                  fileNode->path,
                  expiredReasonAsStr[ fileNode->expiredReason ],
                  watchedTree->exec );
        write( fd, buffer, strlen( buffer ));
        free( buffer );
        close( fd );

        forgetNode( fileNode );

        result = execFileNode( fileNode );

    }
    result = 0;
    return result;
}


/**
 * @brief
 * @param watchedTree
 * @param watchID
 */
void removeWatchID( tWatchedTree * watchedTree, tWatchID watchID )
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
 * @brief scan the expiringList of each watchedTree, expiring entries whose time has passed
 * @return
 */
tError processExpiredFSNodes( void )
{
    int result = 0;

    time_t now = time( NULL );
    for ( tWatchedTree * watchedTree = gEvent.treeList;
          watchedTree != NULL;
          watchedTree = watchedTree->next )
    {
        tFSNode * fsNode;
        while ( result == 0
            && ( fsNode = watchedTree->expiringList) != NULL
            && fsNode->expires <= now)
        {
            /* this will always the 'expired' node at the head of the
             * 'expiringList', hence the unusual loop construct */
            result = fileExpired( fsNode );
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
void doiNotifyDelete( tFSNode * pathNode )
{
    if ( pathNode->type == kFile ) {
        const tWatchedTree * const watchedTree = pathNode->watchedTree;
        if ( unlinkat( watchedTree->shadow.fd, pathNode->relPath, 0 ) == -1 ) {
            logError( "Error: unable to delete the shadow file \'%s%s\'",
                      watchedTree->shadow.path, pathNode->relPath );
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
void doiNotifyCreate( tFSNode * pathNode,
                      const struct inotify_event * event,
                      const char * fullPath )
{
    if ( pathNode->type == kFile ) {
        /* start the idle timer */
        resetExpiration( pathNode, kFirstSeen );
    }
}


/**
 * @brief
 * @param watchedTree
 * @param expires
 * @param pathNode
 */
void doiNotifyCloseWrite( tFSNode * pathNode )
{
    tExpiredReason reason = pathNode->expiredReason;
    if ( reason != kFirstSeen ) {
        reason = kModified;
    }
    resetExpiration( pathNode, reason );
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
void doiNotifyMove( tFSNode * pathNode,
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
void doiNotifyEvent( tFSNode * pathNode,
                     const struct inotify_event * event,
                     const char * fullPath )
{
    if ( pathNode == NULL ) {
        return;
    }

    tWatchedTree * watchedTree = pathNode->watchedTree;
    if ( event->mask & IN_Q_OVERFLOW ) {
        /* dropped some events - rescan ASAP */
        watchedTree->nextRescan = time(NULL );
    }
    if ( event->mask & IN_IGNORED ) {
        /* for whatever reason, the watchID for this node will be invalid
         * after this event, so clean up any references we have to it */
        removeWatchID( watchedTree, event->wd );
    }

    if ( event->mask & IN_CREATE ) {
        doiNotifyCreate( pathNode, event, fullPath );
    } else if ( event->mask & IN_CLOSE_WRITE ) {
        doiNotifyCloseWrite( pathNode );
    } else if ( event->mask & ( IN_MOVED_FROM | IN_MOVED_TO ) ) {
        doiNotifyMove( pathNode, event, fullPath );
    } else if ( event->mask & IN_DELETE ) {
        doiNotifyDelete( pathNode );
    } else { /* all other events */
        if ( pathNode->expires != 0 ) {
            resetExpiration( pathNode, pathNode->expiredReason );
            logDebug( "event has deferred expiration of \'%s\'",
                      pathNode->path );
        }
    }
}


/**
 * @brief
 * @param watchedTree
 * @param event
 * @return
 */
tError processOneInotifyEvent( tWatchedTree * watchedTree, const struct inotify_event *event )
{
    tError result = 0;

    const tFSNode * watchedNode = fsNodeFromWatchID( watchedTree, event->wd );
    if ( watchedNode == NULL ) {
        logError( "internal: couldn't find a FSNode with the watchID %d", event->wd );
        return -ENOENT;
    }

    /*
     * Caution: if event->len is zero, the string at event->name is completely
     * absent - i.e. it's not equivalent to 'strlen(event->name)', it's the
     * number of bytes that event->name occupies.
     *
     * Zero means event->name has a size of zero bytes, NOT that it is an empty
     * string - there is no NULL terminator *at all*. In other words, if
     * event->len is zero, don't use event->name, there's NO C string there.
     */
    char * fullPath;
    if ( event->len > 0 ) {
        asprintf( &fullPath, "%s/%s", watchedNode->path, event->name );
    } else {
        fullPath = strdup( watchedNode->path );
    }

    tFSNode * pathNode = fsNodeFromPath( watchedTree, fullPath );
    if ( pathNode == NULL && !(event->mask & IN_ISDIR)) {
        /*
         * It's a file, and we didn't find its hash of the full path in this
         * watchedTree. Therefore, this must be the first iNotify activity
         * we've seen on this file (recently, at least).
         *
         * So create a fileNode to track iNotify activity. Each fileNode has an
         * expiration, which is restarted when an iNotify event is received for
         * this file. So the timer only expires after the fileNode hasn't
         * received any iNotify events for long enough (i.e. the 'idle'
         * timeout).
         *
         * If/when that happens, the fileNode will be 'expired' by calling
         * fileExpired(), which cleans up and queues the file to be processed.
         */
        struct stat fileInfo;
        if ( stat( fullPath, &fileInfo ) == -1 && errno != ENOENT ) {
            logError( "unable to get information about \'%s\'", fullPath );
        } else {
            /* it exists, so make a new fileNode to track iNotify events for it */
            pathNode = watchNode( watchedTree, fullPath, kFile );
        }
    }

    logEvent( event, watchedNode );

    doiNotifyEvent( pathNode, event, fullPath );

    free( (void *)fullPath );

    return result;
}


/**
 * @brief
 * @param watchedTree
 * @param event
 * @return
 */
tError processInotifyEvents( tWatchedTree * watchedTree )
{
    int result = 0;

    /* Some CPU architectures will fault if you attempt tp read multibyte
     * variables if they are not properly aligned. So for safety's sake, we ask
     * the compiler to align the buffer with 'int', which is the type of the
     * first element of struct inotify_event. The compiler will take care of
     * padding the remaining fields in the rest of the structure so any
     * alignment requirements for that CPU architecture will be honored.
     *
     * A compiler may automagically handle aligning the buffer too, but better
     * safe than sorry. */

    static char buf[8192] __attribute__ ((aligned(__alignof__(int))));

    ssize_t len = read( watchedTree->inotify.fd, buf, sizeof( buf ));

    /* If the nonblocking read() found nothing to read, then it returns
     * -1 with errno set to EAGAIN. That's a normal case, not an error */
    if ( len == -1 && errno != EAGAIN ) {
        logError( "unable to read from iNotify fd %d", watchedTree->inotify.fd );
        result = -errno;
    } else {
        /* Loop over all iNotify events packed into the buffer we just filled */
        int count = 0;
        const char * event = buf;
        const char * end = buf + len;
        while ( event < end ) {
            ++count;
            result = processOneInotifyEvent( watchedTree, (const struct inotify_event *)event );
            event += sizeof( struct inotify_event ) + ((struct inotify_event *)event)->len;
        }
        logDebug( "%d iNotify events in a buffer %ld bytes long", count, len );
    }

    return result;
}


/**
 * @brief
 * @param epollEvent
 * @return
 */
int processEpollEvent( const struct epoll_event * const epollEvent )
{
    int result = 0;

    logDebug( "epoll event 0x%x, %p", epollEvent->events, epollEvent->data.ptr );
    /* Loop over the epoll events we just read from the epoll file descriptor. */
    if ( epollEvent->events & EPOLLIN )
    {
        /* check for the 'special values' */
        switch ( epollEvent->data.u64 )
        {
        case kExecTrigger:
            logDebug( "### exec triggered ###");
            break;

        case kExitLoop:
            logDebug( "### exit the loop ###");
            result = -ECANCELED;
            break;

        default:
            /* if it's not a 'special' event, then iNotify events are waiting,
             * and data.ptr points at the corresponding watchedTree */
            processInotifyEvents((tWatchedTree *)epollEvent->data.ptr );
            break;
        }
    }

    return result;
}


/**
 * @brief
 * @return
 */
tError checkRescans( void )
{
    tError result = 0;

    for ( tWatchedTree * watchedTree = gEvent.treeList;
          watchedTree != NULL;
          watchedTree = watchedTree->next ) {
        /* re-scan each watchedTree hierarchy periodically, since iNotify isn't 100% reliable */
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
 * @param watchedTree
 * @return
 */
tError makeShadowDir( tWatchedTree * watchedTree )
{
    tError result = 0;

    asprintf( (char **)&(watchedTree->shadow.path), "%s.seen/", watchedTree->root.path );
    if (watchedTree->shadow.path != NULL ) {
        watchedTree->shadow.pathLen = strlen( watchedTree->shadow.path );

        if ( mkdir( watchedTree->shadow.path, S_IRWXU | S_IRWXG) == -1 && errno != EEXIST) {
            logError( "unable to create shadow directory \'%s\'", watchedTree->shadow.path );
            result = -errno;
        } else {
            watchedTree->shadow.fd = open( watchedTree->shadow.path, O_DIRECTORY );
            if ( watchedTree->shadow.fd == -1 ) {
                logError( "couldn't open shadow directory \'%s\'", watchedTree->shadow.path );
                result = -errno;
            }
        }
    }

    return result;
}


/**
 * @brief
 * @param root
 * @return
 */
tError openRootDir( tWatchedTree * watchedTree, const char * dir )
{
    tError result = 0;

    char * rootPath = realpath( dir, NULL);
    // watchedTree->root.path =
    if ( rootPath == NULL ) {
        logError( "unable to normalize the root path \'%s\'", dir );
        result = -errno;
    } else {
        struct stat fileInfo;
        if ( stat( rootPath, &fileInfo ) == -1 ) {
            result = -errno;
            logError( "couldn't get info about %s", watchedTree->root.path );
        } else {
            if ( S_ISDIR( fileInfo.st_mode ) ) {
                size_t len = strlen( rootPath );
                rootPath = realloc( rootPath, len + 2 );
                rootPath[len++] = '/';
                rootPath[len]   = '\0';
                watchedTree->root.path    = rootPath;
                watchedTree->root.pathLen = len;

                logDebug( "absolute root.path is \'%s\'", watchedTree->root.path );

                watchedTree->root.fd = open( watchedTree->root.path, O_DIRECTORY );
                if ( watchedTree->root.fd < 0 ) {
                    logError( "couldn't open the root directory \'%s\'", watchedTree->root.path );
                    result = -errno;
                }
            }
        }
    }
    return result;
}

#if 0
tError childEvent( const struct epoll_event * event )
{
    tError result = 0;
    (void)event;
    return result;
}


/**
 * @brief
 * @param pipeReadFd
 * @return
 */
tError child( int pipeReadFd )
{
    tError result = 0;

    const int eventsSize = 5;
    struct epoll_event events[ eventsSize ];

    int epollFd = epoll_create( eventsSize );
    if ( epollFd == -1 ) {
        result = -errno;
        logError( "failed to create epoll file descriptor" );
    } else {
        if ( epoll_ctl( epollFd, EPOLL_CTL_ADD, pipeReadFd, events ) == -1 ) {
            result = -errno;
            logError( "unable to add pipe fd to epoll" );
        }
    }
    while ( result == 0 ) {
        int count = epoll_wait( epollFd, events, eventsSize, 60000 );
        if ( count == -1 ) {
            result = -errno;
            logError( "epoll_wait() returned an error" );
        } else {
            for (int i = 0; i < count; ++i ) {
                result = childEvent( &events[ i ] );
            }
        }
    }

    return result;
}
#endif


/**
 * @brief
 * @param watchedTree
 * @return
 */
tError registerForInotifyEvents( tWatchedTree * watchedTree )
{
    tError result = 0;

    watchedTree->inotify.fd = inotify_init();
    if ( watchedTree->inotify.fd == -1 ) {
        logError( "Unable to register for filesystem events" );
        result = -errno;
    } else {
        struct epoll_event epollEvent;
        epollEvent.events   = EPOLLIN;
        epollEvent.data.ptr = watchedTree;

        if ( epoll_ctl( gEvent.epoll.fd,
                        EPOLL_CTL_ADD,
                        watchedTree->inotify.fd,
                        &epollEvent ) == -1 ) {
            logError( "unable to register inotify.fd fd %d with epoll fd %d",
                      watchedTree->inotify.fd, gEvent.epoll.fd );
            result = -errno;
        }
    }

    return result;
}


/**
 * @brief
 * @param dir
 * @return
 */
tError createTree( const char * dir, const char * exec )
{
    int result = 0;

    if ( dir == NULL ) return -EINVAL;

    tWatchedTree * watchedTree = calloc( 1, sizeof( tWatchedTree ));
    if ( watchedTree == NULL ) {
        result = -ENOMEM;
    } else {
        logDebug( "creating tree for \'%s\'", dir );

        /* since calloc() was used for this structure, the pointers it contains are already NULL */

        watchedTree->exec = strdup( exec );

        result = openRootDir( watchedTree, dir );
        if ( result == 0 ) {
            result = makeShadowDir( watchedTree );

            logDebug( "root.fd: %d, shadow.fd: %d", watchedTree->root.fd, watchedTree->shadow.fd );
        }

        if (result == 0) {
            result = registerForInotifyEvents( watchedTree );
        }

        if ( result == 0 ) {
            watchedTree->nextRescan = time( NULL ) + 1;

            watchedTree->next = gEvent.treeList;
            gEvent.treeList   = watchedTree;
        } else {
            free( (void *)watchedTree->exec );
            free( (void *)watchedTree->root.path );
            free( (void *)watchedTree->shadow.path );
            free( watchedTree );
        }
    }

    return result;
}


/**
 * @brief
 * @param pid
 * @return
 */
tError createPidFile( pid_t pid )
{
    tError result = 0;

    if ( g.pidFilename == NULL ) {
        char * pidDir;
        asprintf( &pidDir, "/tmp/%s", g.executableName );
        if ( mkdir( pidDir, S_IRWXU) == -1 && errno != EEXIST ) {
            result = -errno;
            logError( "Error: unable to create directory %s", pidDir );
        } else {
            asprintf( &g.pidFilename,"/tmp/%s/%s.pid",
                      g.executableName, g.executableName );
        }
        free( pidDir );
    }

    if ( g.pidFilename != NULL ) {
        FILE * pidFile = fopen( g.pidFilename, "w" );
        if ( pidFile == NULL ) {
            logError( "Error: unable to open %s for writing", g.pidFilename );
            result = -errno;
        } else {
            if ( fprintf( pidFile, "%d", pid ) == -1 ) {
                logError( "Error: unable to write pid to %s", g.pidFilename );
                result = -errno;
            }
            fclose( pidFile );
        }
    }

    return result;
}

/**
 * @brief
 * @return
 */
pid_t getDaemonPID( void )
{
    pid_t result;

    FILE * pidFile = fopen( g.pidFilename, "r" );
    if ( pidFile == NULL ) {
        logError( "Error: unable to open %s for reading",
                  g.pidFilename );
        result = -errno;
    } else {
        if ( fscanf( pidFile, "%d", &result ) == -1 || result == 0 ) {
            logError( "Error: unable to read pid from %s",
                      g.pidFilename );
            result = -errno;
        }
        fclose( pidFile );
    }

    return result;
}

/**
 * @brief
 */
void removePIDfile( void )
{
    if ( g.pidFilename != NULL ) {
        unlink( g.pidFilename );
        free(g.pidFilename );
        g.pidFilename = NULL;
    }
}

/**
 * @brief
 * @return
 */
tError eventLoop( void )
{
    int                result = 0;
    struct epoll_event epollEvents[32];

    do {
        int timeout = (int)(nextExpiration() - time( NULL ));

        /* * * * * * block waiting for events or a timeout * * * * * */
        errno = 0;
        logDebug( "epoll_wait( fd: %d, %p[%ld], %d secs )",
                  gEvent.epoll.fd,
                  epollEvents,
                  sizeof( epollEvents ) / sizeof( struct epoll_event ),
                  timeout );
        int count = epoll_wait( gEvent.epoll.fd,
                                epollEvents,
                                sizeof( epollEvents ) / sizeof( struct epoll_event ),
                                timeout * 1000 );
        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
        logInfo( "timestamp %ld, count %d, errno %d", time( NULL ), count, errno );
        if ( count < 0 ) {
            switch ( errno ) {
            case EINTR:
            case EAGAIN:
                /* harmless */
                break;

            default:
                logError( "epoll() returned %d", count );
                result = -1;
                break;
            }
        } else {
            int i = 0;
            while ( i < count && result == 0 ) {
                result = processEpollEvent( &epollEvents[ i ] );
                i++;
            }

            if ( result == 0 ) {
                result = processExpiredFSNodes();
            }

            if ( result == 0 ) {
                result = checkRescans();
            }
        }

    } while ( result == 0 );

    logInfo( "loop terminated %d", result );
    removePIDfile();
    return result;
}

/**
 * @brief
 */
void stopDaemon( void )
{
    logCheckpoint();
    write( gEvent.pipe.exitLoop[kWriteEnd], "!", 1 );
}


/**
 * @brief
 */
void daemonExit( void )
{
    logInfo( "daemonExit()" );
    stopDaemon();
}


/**
 * @brief
 * @param signum
 */
void daemonSignal( int signum )
{

#ifdef DEBUG
    const char * signalAsStr[] = {
        [SIGHUP]  = "Hangup",
        [SIGINT]  = "Interactive attention signal",
        [SIGQUIT] = "Quit",
        [SIGILL]  = "Illegal instruction",
        [SIGTRAP] = "Trace/breakpoint trap",
        [SIGABRT] = "Abnormal termination",
        [SIGFPE]  = "Erroneous arithmetic operation",
        [SIGKILL] = "Killed",
        [SIGSEGV] = "Invalid access to storage",
        [SIGPIPE] = "Broken pipe",
        [SIGALRM] = "Alarm clock",
        [SIGTERM] = "Termination request"
    };

    const char * sigDesc = "(unknown)";
    if ( signum <= SIGTERM && signalAsStr[ signum ] != NULL ) {
        sigDesc = signalAsStr[ signum ];
    }
    logInfo( "daemon received signal %d: %s", signum, sigDesc );
#endif

    stopDaemon();
}


/**
 * @brief
 * @return
 */
tError registerHandlers( void )
{
    tError result = 0;

    /* register a handler for SIGTERM to remove the pid file */
    struct sigaction new_action;
    struct sigaction old_action;

    sigemptyset( &new_action.sa_mask );
    new_action.sa_handler = daemonSignal;
    new_action.sa_flags   = 0;

    sigaction( SIGTERM, &new_action, &old_action );

    /* and if we exit normally, also remove the pid file */
    if ( atexit( daemonExit ) != 0 ) {
        logError( "Error: daemon failed to register an exit handler" );
        result = -errno;
    }

    return result;
}


/**
 * @brief
 * @return
 */
tError initEpoll( void )
{
    tError result = 0;
    gEvent.epoll.fd = epoll_create1( 0 );

    struct epoll_event epollEvent;

    if ( pipe2( gEvent.pipe.exitLoop, 0 ) == -1 ) {
        result = -errno;
        logError( "failed to create exitLoop pipe" );
    } else {
        epollEvent.events = EPOLLIN;
        epollEvent.data.u64 = kExitLoop;
        if ( epoll_ctl( gEvent.epoll.fd,
                        EPOLL_CTL_ADD,
                        gEvent.pipe.exitLoop[kReadEnd],
                        &epollEvent ) == -1 )
        {
            result = -errno;
            logError( "unable to register exitloop fd %d with epoll fd %d",
                      gEvent.pipe.exitLoop[kReadEnd], gEvent.epoll.fd );
        }
    }

    if ( pipe2( gEvent.pipe.execTrigger, 0 ) == -1 ) {
        result = -errno;
        logError( "failed to create execTrigger pipe" );
    } else {
        epollEvent.events = EPOLLIN;
        epollEvent.data.u64 = kExecTrigger;


        if ( epoll_ctl( gEvent.epoll.fd,
                        EPOLL_CTL_ADD,
                        gEvent.pipe.execTrigger[kReadEnd],
                        &epollEvent ) == -1 )
        {
            logError( "unable to register execTrigger fd %d with epoll fd %d",
                      gEvent.pipe.execTrigger[kReadEnd], gEvent.epoll.fd );
            result = -errno;
        }
    }

    return result;
}


/**
 * @brief
 * @return
 */
tError initDaemon( void )
{
    logDebug( "%s", __func__ );
    tError result;
    pid_t pid = getpid();

    result = createPidFile( pid );

    if ( result == 0 ) {
        result = initEpoll();
    }

    if ( result == 0 ) {
        result = registerHandlers();
    }

    return result;
}

/**
 * @brief gets the party started
 * Called after all the watchedTrees have been created.
 * @return
 */
tError startDaemon( void )
{
    tError result;

    result = eventLoop();

    return result;
}