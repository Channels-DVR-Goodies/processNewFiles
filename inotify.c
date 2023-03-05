//
// Created by paul on 12/10/22.
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
#include "inotify.h"


const char * const fsTypeAsStr[] = {
    [kUnset]     = "(unset)",
    [kTree]      = "tree",
    [kDirectory] = " dir",
    [kFile]      = "file"
};


/**
 * @brief
 * @param buffer
 * @param bufferSize
 * @param event
 * @return
 */
const char * inotifyEventTypeAsStr( const struct inotify_event * event )
{

    static struct {
        int mask;
        const char * label;
    } iNotifyFlags[] = {
        { IN_ACCESS,       "Access" },      /* File was accessed.  */
        { IN_MODIFY,       "Modify" },      /* File was modified.  */
        { IN_ATTRIB,       "Attrib" },      /* Metadata changed.  */
        { IN_CLOSE_WRITE,  "Cl_Wr" },       /* File opened for writing was closed.  */
        { IN_CLOSE_NOWRITE,"Cl_NoWr" },     /* File opened for reading was closed.  */
        { IN_OPEN,         "Open" },        /* File was opened.  */
        { IN_MOVED_FROM,   "Mv_From" },     /* File was moved from <src>.  */
        { IN_MOVED_TO,     "Mv_To" },       /* File was moved to <dest>.  */
        { IN_CREATE,       "Create" },      /* File was created in dir.  */
        { IN_DELETE,       "Delete" },      /* File was deleted from dir.  */
        { IN_DELETE_SELF,  "Del_Self" },    /* Self was deleted.  */
        { IN_MOVE_SELF,    "Mv_Self" },     /* Self was moved.  */
        /* Flags set by kernel */
        { IN_UNMOUNT,      "Unmount" },     /* Backing fs was unmounted.  */
        { IN_Q_OVERFLOW,   "Q_Overflow" },  /* Event queued overflowed.  */
        { IN_IGNORED,      "Ignored" },     /* File was ignored.  */
        { IN_ISDIR,        "IsDir" },       /* Event refers to a directory */
        /* mark the end of the table */
        { 0, NULL }
    };


    size_t bufferSize = 1;  /* for the trailing null */
    for ( int i = 0; iNotifyFlags[ i ].mask != 0; ++i ) {
        if ( event->mask & iNotifyFlags[ i ].mask ) {
            bufferSize += strlen( iNotifyFlags[ i ].label ) + sizeof(" | ");
        }
    }

    char * buffer = (char *)calloc( 1, bufferSize );

    if ( buffer == NULL ) return NULL;

    bool needSeparator = false;
    for ( int i = 0; iNotifyFlags[ i ].mask != 0; ++i ) {
        if ( event->mask & iNotifyFlags[ i ].mask ) {
            if ( needSeparator ) {
                strncat( buffer, " | ", bufferSize );
            }
            strncat( buffer, iNotifyFlags[ i ].label, bufferSize );
            needSeparator = true;
        }
    }

    return buffer;
}


/**
 * @brief
 * @param buffer
 * @param remaining
 * @param watchedTree
 * @param fsNode
 */
const char * fsNodeAsStr( const tFSNode * fsNode )
{
    char * result = NULL;

    const char * emptyStr = "";
    /* Print the name of the watched node */
    if ( fsNode != NULL )
    {
        const char * cookieStr = emptyStr;
        if ( fsNode->cookie != 0 ) {
            if ( asprintf( (char **)&cookieStr, " {%u}", fsNode->cookie ) < 1 ) {
                logError( "unable to generate cookie message");
            }
        }

        const char * expiresStr = emptyStr;
#if 0
        if ( fsNode->expires != 0 )
        {
            if ( asprintf( (char **)&expiresStr,
                           " %s expires in %lu secs",
                           expiredReasonAsStr[ fsNode->expiredReason ],
                           fsNode->expires - time(NULL ) ) < 1 ) {
                logDebug( "unable to generate timeout message" );
            }
        }
#endif

        if ( asprintf( &result,
                       "%s [%02d] \'%s\'%s%s",
                       fsTypeAsStr[ fsNode->type ],
                       fsNode->watchID,
                       fsNode->path,
                       cookieStr,
                       expiresStr ) < 1 ) {
            logDebug( "unable to generate debug message" );
        }

        if ( cookieStr  != emptyStr ) free( (void *)cookieStr );
        if ( expiresStr != emptyStr ) free( (void *)expiresStr );
    }

    return result;
}


/**
 * @brief
 * @param watchedTree
 * @param fsNode
 */
void logFsNode( const tFSNode * fsNode )
{
    const char * buffer;

    buffer = fsNodeAsStr( fsNode );
    logDebug( "%s", buffer );
    free( (void *)buffer );
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
    if ( event != NULL && !(event->mask & IN_ISDIR) )
    {
        /* display event type. */
        const char * eventTypeStr = inotifyEventTypeAsStr( event );

        char * cookieStr = NULL;
        if ( asprintf( &cookieStr, " (%u)", event->cookie ) < 1 ) {
            logDebug( "unable to generate debug message" );
        }
        if ( event->cookie == 0 ) {
            cookieStr[0] = '\0';
        }

        /* the name of the file, if any. */
        const char * nameStr = event->name;
        if ( event->len == 0 ) {
            nameStr = "";
        }

        const char * nodeStr = NULL;

        const tWatchedTree * watchedTree = watchedNode->watchedTree;
        if ( watchedTree != NULL ) {
            tFSNode * fsNode;
            HASH_FIND( watchHandle, watchedTree->watchHashMap, &event->wd, sizeof( tWatchID ), fsNode );
            if ( fsNode == NULL ) {
                logError( "Error: watchID [%02d] not found", event->wd );
            } else {
                nodeStr = fsNodeAsStr( watchedNode );
            }
        }
        logDebug( "[%02u] %-20s %-8s %s (%s)", event->wd, eventTypeStr, cookieStr, nodeStr, nameStr );

        free( (void *)eventTypeStr );
        free( (void *)nodeStr );
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
 * @brief function to ensure expiringList is in ascending order of expiration
 * @param newEntry
 * @param existingEntry
 * @return
 */
int orderedByExpiration(const tFSNode * newEntry, const tFSNode * existingEntry )
{
    if (newEntry->expires.at < existingEntry->expires.at ) {
        return -1;
    }
    if (newEntry->expires.at > existingEntry->expires.at ) {
        return 1;
    }
    return 0;
}


/**
 * @brief
 * @param watchedTree
 * @param node
 * @param reason
 * @param node
 */
void resetExpiration(tFSNode * node, tExpiredReason reason )
{
    if (node != NULL)
    {
        time_t when = time(NULL ) + node->expires.wait;

        logDebug( "resetExpiration of %s", node->relPath );
        node->expires.because = reason;
        if (node->expires.at != when ) {
            if ( g.expiringList != NULL ) {
                LL_DELETE2(g.expiringList, node, next );
            }
            node->expires.at = when;
            LL_INSERT_INORDER2(g.expiringList, node, orderedByExpiration, next );
        }
    }
}


/**
 * @brief
 * @param watchedTree
 * @param fullPath
 * @return
 */
tFSNode * fsNodeFromPath( tWatchedTree * watchedTree, const char * fullPath, tFSNodeType type )
{
    tFSNode * node;

    if ( strncmp( fullPath, watchedTree->shadow.path, watchedTree->shadow.pathLen ) == 0 ) {
        /* don't generate nodes for anything in the shadow hierarchy */
        return NULL;
    }

    tHash hash = calcHash( fullPath );
    HASH_FIND(pathHandle, watchedTree->pathHashMap, &hash, sizeof( tHash ), node );

    if ( node == NULL ) {
        node = (tFSNode *)calloc(1, sizeof( tFSNode ) );
        node->type        = type;
        node->watchedTree = watchedTree;
        node->path        = strdup(fullPath );
        node->pathHash    = calcHash(node->path );
        node->relPath     = &node->path[ watchedTree->root.pathLen ];
        if ( *node->relPath == '/' ) {
            ++node->relPath;
        }

        HASH_ADD(pathHandle, watchedTree->pathHashMap, pathHash, sizeof( tHash ), node );

        switch (type)
        {
        case kFile:
            node->expires.wait  = g.timeout.idle;
            resetExpiration(node, kFirstSeen );
            break;

        case kDirectory:
            node->watchID = inotify_add_watch(watchedTree->inotify.fd, fullPath, IN_ALL_EVENTS);
            logInfo("watch [%d] %s", node->watchID, fullPath);
            if (node->watchID == -1 ) {
                logError( "problem watching directory \'%s\'", fullPath );
            } else {
                HASH_ADD(watchHandle, watchedTree->watchHashMap, watchID, sizeof( tWatchID ), node );
            }
            break;

        default:
            break;
        }

        // logFsNode( node );
    }

    return node;
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
 *
 * @param fsNode
 */
void forgetWatch(const tFSNode *fsNode)
{
    if (fsNode != NULL && fsNode->watchID != 0 ) {
        inotify_rm_watch( fsNode->watchedTree->inotify.fd, fsNode->watchID );
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

    logDebug( "remove \'%s\' [%02d]", fsNode->path, fsNode->watchID );

    const tWatchedTree * const watchedTree = fsNode->watchedTree;
    if ( fsNode->type == kFile ) {
        /* if a file is created and then deleted before it's ever processed, then
         * it won't be present in the shadow hierarchy yet - and that's normal */
        if ( unlinkat( watchedTree->shadow.fd, fsNode->relPath, 0 ) == -1 && errno != ENOENT ) {
            logError( "failed to delete \'%s\'", fsNode->path );
        }
        logSetErrno( 0 );
    }

    forgetNode( fsNode );
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
void doiNotifyCreate( tFSNode * pathNode )
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
    tExpiredReason reason = pathNode->expires.because;
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
        /* dropped events - so force full rescan ASAP */
        rescanAllTrees();
    }

    if ( event->mask & IN_IGNORED ) {
        /* for whatever reason, the watchID for this node will be invalid
         * after this event, so clean up any references we have to it */
        removeWatchID( watchedTree, event->wd );
    }

    if ( event->mask & IN_CREATE ) {
        doiNotifyCreate( pathNode );
    } else if ( event->mask & IN_CLOSE_WRITE ) {
        doiNotifyCloseWrite( pathNode );
    } else if ( event->mask & ( IN_MOVED_FROM | IN_MOVED_TO ) ) {
        doiNotifyMove( pathNode, event, fullPath );
    } else if ( event->mask & IN_DELETE ) {
        doiNotifyDelete( pathNode );
    } else { /* all other events */
        if ( pathNode->expires.at != 0 ) {
            resetExpiration( pathNode, pathNode->expires.because );
            logDebug( "event delayed expiration of \'%s\'",
                      pathNode->relPath );
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

    logEvent( event, watchedNode );

    /*
     * Caution: if event->len is zero, the string at event->name is completely absent
     * - i.e. it's not equivalent to 'strlen(event->name)', it's the number of bytes
     * that event->name occupies.
     *
     * Zero means event->name has a size of zero bytes, NOT that it is an empty
     * string - there is no NULL terminator *at all*. In other words, if
     * event->len is zero, don't use event->name, there's NO C string there.
     */

    char * fullPath;
    if ( event->len > 0 ) {
        if ( asprintf( &fullPath, "%s/%s", watchedNode->path, event->name ) < 1 ) {
            logDebug( "unable to generate full path" );
        }
    } else {
        fullPath = strdup( watchedNode->path );
    }

    // logInfo( "path: \'%s\' ", fullPath );
    tFSNode * pathNode = fsNodeFromPath( watchedTree,
                                         fullPath,
                                         (event->mask & IN_ISDIR) ? kDirectory : kFile );

    if ( pathNode != NULL ) {
        doiNotifyEvent( pathNode, event, fullPath );
    }

    free( (void *)fullPath );

    return result;
}


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
        if ( registerFdToEpoll( watchedTree->inotify.fd, (uint64_t)watchedTree ) == -1 ) {
            logError( "unable to register inotify fd %d",
                      watchedTree->inotify.fd );
            result = -errno;
        }
    }

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
        logDebug( "processed %d iNotify events from a %ld byte buffer", count, len );
    }

    return result;
}
