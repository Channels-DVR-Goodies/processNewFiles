//
// Created by paul on 6/5/22.
//

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>
#include <errno.h>
#include <linux/limits.h>

#include <signal.h>
#include <string.h>
#include <dlfcn.h>
#include <poll.h>
#include <sys/epoll.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <ftw.h>
#include <sys/inotify.h>

#include <argtable3.h>
#include <uthash.h>
#include <utlist.h>
#include <libconfig.h>

#include "logStuff.h"

typedef unsigned char byte;
typedef unsigned long tHash;
typedef uint32_t      tCookie;
typedef int           tWatchID;
typedef int           tFileDscr;
typedef int           tError;
typedef int           tPipe[2];
enum { kPipeReadFD = 0, kPipeWriteFD = 1 };

typedef enum { kRescan=0, kNew, kModified, kMoved } tExpiredAction;

const char * expiredActionAsStr[] = {
    [kRescan]    = "rescan",
    [kNew]       = "new",
    [kModified]  = "modified",
    [kMoved]     = "moved"
};

typedef struct {
    tExpiredAction action;
    const char path[PATH_MAX];
} tExecMsg;

typedef struct nextNode {
    enum { kDirectory,
           kFile }      type;
    tWatchID            watchID;
    tCookie             cookie;
    const char *        path;
    tHash               pathHash;
    tExpiredAction      expiredAction;
    time_t              expires;

    struct nextNode *   expiringNext;
    UT_hash_handle      watchh, pathh, cookieh;
} tFSNode;

typedef struct nextWatchedTree
{
    struct nextWatchedTree * next;

    tFSNode *       watchHashes;    // hashmap of the watchID
    tFSNode *       pathHashes;     // hashmap of the full rootPath, hashed
    tFSNode *       cookieHashes;   // hashmap of the cookie values (as used for IN_MOVED event pairs
    tFSNode *       expiringList;   // linked list ordered by ascending expiration time

    time_t          nextRescan;

    struct {
        tFileDscr    fd;
    } inotify;

    struct {
        const char * path;
        size_t       pathLen;
        tFileDscr    fd;
    } root;
    struct {
        const char * path;
        size_t       pathLen;
        tFileDscr    fd;
    } seen;

} tWatchedTree;

struct {
    const char *    executableName;

    bool            running;
    tPipe           pipe;   // communication with the executor

    const char *    pidFilename;

    int             epollfd;
    tWatchedTree *  treeList;
    tWatchedTree *  watchedTree;

    config_t *      config;
    struct
    {
        struct arg_lit  * help;
        struct arg_lit  * version;
        struct arg_lit  * killDaemon;
        struct arg_lit  * zero;
        struct arg_lit  * foreground;
        struct arg_int  * debugLevel;
        struct arg_file * path;
        struct arg_end  * end;  // must be last !
    } option;
} g;

tHash calcHash( const char * string )
{
    tHash result = 0xDeadBeef;
    const char * p = string;
    while ( *p != '\0' )
    {
        result = (result * 43) ^ *p++;
    }
    return result;
}


const char * toRelativePath( const tWatchedTree * watchedTree, const char * fullPath )
{
    const char * result = &fullPath[ watchedTree->root.pathLen ];
    if ( *result == '/' ) ++result;
    return result;
}

int orderedByExpiration( const tFSNode * newNode, const tFSNode * existingNode )
{
    if ( newNode->expires < existingNode->expires ) return -1;
    if ( newNode->expires > existingNode->expires ) return 1;
    return 0;
}

size_t appendStr( char * buffer, size_t remaining, const char ** separator, const char * string )
{
    size_t len;
    if (remaining > 0)
    {
        strncat( buffer, *separator, remaining );
        len = strlen( *separator );
        if (len <= remaining) remaining -= len;
        else remaining = 0;
    }

    if (remaining > 0)
    {
        strncat(buffer, string, remaining );
        len = strlen( string );
        if (len <= remaining) remaining -= len;
        else remaining = 0;
    }
    *separator = " | ";

    return remaining;
}

size_t inotifyEventTypeAsStr( char * buffer, size_t remaining, const struct inotify_event * event)
{
    const char * separator = " ";

    buffer[0] = '\0';

    /* Supported events suitable for MASK parameter of INOTIFY_ADD_WATCH.  */
    if ( event->mask & IN_ACCESS ) /* File was accessed.  */
        remaining = appendStr( buffer,  remaining, &separator, "IN_ACCESS");
    if ( event->mask & IN_MODIFY ) /* File was modified.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_MODIFY");
    if ( event->mask & IN_ATTRIB ) /* Metadata changed.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_ATTRIB");
    if ( event->mask & IN_CLOSE_WRITE ) /* Writtable file was closed.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_CLOSE_WRITE");
    if ( event->mask & IN_CLOSE_NOWRITE ) /* Unwrittable file closed.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_CLOSE_NOWRITE");
    if ( event->mask & IN_OPEN ) /* File was opened.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_OPEN");
    if ( event->mask & IN_MOVED_FROM ) /* File was moved from X.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_MOVED_FROM");
    if ( event->mask & IN_MOVED_TO ) /* File was moved to Y.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_MOVED_TO");
    if ( event->mask & IN_CREATE ) /* Subfile was created.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_CREATE");
    if ( event->mask & IN_DELETE ) /* Subfile was deleted.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_DELETE");
    if ( event->mask & IN_DELETE_SELF ) /* Self was deleted.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_DELETE_SELF");
    if ( event->mask & IN_MOVE_SELF ) /* Self was moved.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_MOVE_SELF");

    /* Events sent by the kernel.  */
    if ( event->mask & IN_UNMOUNT ) /* Backing fs was unmounted.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_UNMOUNT");
    if ( event->mask & IN_Q_OVERFLOW ) /* Event queued overflowed.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_Q_OVERFLOW");
    if ( event->mask & IN_IGNORED ) /* File was ignored.  */
    	remaining = appendStr( buffer,  remaining, &separator, "IN_IGNORED");
    if ( event->mask & IN_ISDIR ) /* refers to a directory */
        remaining = appendStr( buffer,  remaining, &separator, "IN_ISDIR");

    return remaining;
}

void fsNodeAsStr( char * buffer, size_t remaining, const tWatchedTree * watchedTree, const tFSNode * fsNode )
{
    /* Print the name of the watched directory. */
    if ( fsNode != NULL)
    {
        char pathStr[PATH_MAX];
        snprintf( pathStr, sizeof(pathStr), "%.*s{%s}",
            (int)watchedTree->root.pathLen,
            fsNode->path,
            &fsNode->path[ watchedTree->root.pathLen ]);

        char cookieStr[32];
        cookieStr[0] = '\0';
        if ( fsNode->cookie != 0 )
        {
            snprintf( cookieStr, sizeof(cookieStr), " {%u}", fsNode->cookie);
        }

        char expiresStr[32];
        expiresStr[0] = '\0';
        if ( fsNode->expires != 0 )
        {
            snprintf( expiresStr, sizeof(expiresStr), " %s action in %lu secs",
                      expiredActionAsStr[fsNode->expiredAction],
                      fsNode->expires - time(NULL) );
        }

        snprintf( buffer, remaining,
                  "[%02d] \'%s\'%s%s",
                  fsNode->watchID,
                  pathStr,
                  cookieStr,
                  expiresStr );
    }
}

void displayFsNode( const tWatchedTree * watchedTree, const tFSNode * fsNode )
{
    char buffer[PATH_MAX+128];
    size_t remaining = sizeof(buffer);
    fsNodeAsStr( buffer, remaining, watchedTree, fsNode );
    logDebug( "%s", buffer);
}

void displayEvent( tWatchedTree * watchedTree, const tFSNode * watchedNode, const struct inotify_event * event )
{
    if (event != NULL)
    {
        char   eventTypeStr[42];
        size_t remaining = sizeof(eventTypeStr) - 1;

        /* display event type. */
        remaining = inotifyEventTypeAsStr( eventTypeStr, remaining, event );

        char cookieStr[32];
        cookieStr[0] = '\0';
        if ( event->cookie != 0 )
        {
            snprintf( cookieStr, sizeof(cookieStr),  " (%u)", event->cookie );
        }

        /* the name of the file, if any. */
        const char * nameStr = "";
        if ( event->len )
        {
            nameStr = event->name;
        }

        char nodeStr[PATH_MAX];
        nodeStr[0] = '\0';
        if (watchedTree != NULL)
        {

            tFSNode * fsNode;
            HASH_FIND( watchh, watchedTree->watchHashes, &event->wd, sizeof(tWatchID), fsNode);
            if ( fsNode == NULL)
            {
                logError( "Error: watchID [%02d] not found", event->wd );
            }
            else
            {
                fsNodeAsStr( nodeStr, sizeof(nodeStr), watchedTree, watchedNode );
            }
        }
        logDebug( "[%02u] %-32s %-8s %-20s %s", event->wd, eventTypeStr, cookieStr, nameStr, nodeStr );
    }
}

#if 0
void displayHash( tWatchedTree * watchedTree )
{
    int count = HASH_CNT( watchh,watchedTree->watchHashes );
    logError( "count: %d", count );

    for ( tFSNode * wn = watchedTree->watchHashes; wn != NULL; wn = wn->watchh.next )
    {
        displayFsNode( watchedTree, wn );
    }
}
#endif

const char * catPath( const char * front, const char * back )
{
    char * result;
    if ( front == NULL )
    {
        result = NULL;
    }
    else if ( back == NULL || strlen(back) == 0 )
    {
        result = strdup( front );
    }
    else
    {
        size_t l = strlen( front ) + 1 + strlen(back ) + 1;
        result = calloc( l, sizeof(char) );
        if ( result != NULL )
        {
            strncpy( result, front, l );
            strncat( result, "/",   l );
            strncat( result, back,  l );
        }
    }
    return (const char *)result;
}


tError makeSeenDir( tWatchedTree * watchedTree )
{
    int result = 0;

    /* ToDo: make this a config option */
    const char * subdir = ".seen";

    watchedTree->seen.path    = catPath( watchedTree->root.path, subdir );
    watchedTree->seen.pathLen = strlen( watchedTree->seen.path );
    if ( mkdir( watchedTree->seen.path, S_IRWXU | S_IRWXG) < 0 && errno != EEXIST )
    {
        logError( "Error: couldn't create subdirectory \'%s\'", watchedTree->seen.path );
        result = -errno;
    } else {
        errno = 0;
        watchedTree->seen.fd = openat( watchedTree->root.fd, subdir, O_DIRECTORY );
        if ( watchedTree->seen.fd < 0 )
        {
            logError( "Error: couldn't open subdirectory \'%s\'", watchedTree->seen.path );
            result = -errno;
        }
    }
    return result;
}


void setExpiration( tWatchedTree * watchedTree, tFSNode * fsNode, tExpiredAction action, time_t expires )
{
    fsNode->expiredAction = action;
    if ( fsNode->expires != expires )
    {
        if (watchedTree->expiringList != NULL)
        {
            LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
        }
        fsNode->expires = expires;
        if ( expires > 0 )
        {
            LL_INSERT_INORDER2( watchedTree->expiringList, fsNode, orderedByExpiration, expiringNext );
        }
    }
}


tFSNode * makeFileNode( tWatchedTree * watchedTree,
                        const char * fullPath,
                        tExpiredAction action,
                        time_t expires )
{
    tHash hash = calcHash( fullPath );

    tFSNode * fileNode = NULL;
    HASH_FIND( pathh, watchedTree->pathHashes, &hash, sizeof(tHash), fileNode);
    if ( fileNode == NULL)
    {
        // didn't find the hash - must be a new one, so make a matching fsNode
        fileNode = calloc( 1, sizeof(tFSNode));
        if (fileNode != NULL)
        {
            fileNode->type      = kFile;
            fileNode->path      = strdup( fullPath );
            fileNode->pathHash  = calcHash( fileNode->path );
            HASH_ADD( pathh, watchedTree->pathHashes, pathHash, sizeof(tHash), fileNode );

            char nodeStr[PATH_MAX];
            fsNodeAsStr( nodeStr, sizeof(nodeStr), watchedTree, fileNode );
            logDebug( "new fileNode: %s", nodeStr );
        }
    }
    if (fileNode != NULL)
    {
        setExpiration( watchedTree, fileNode, action, expires );
    }
    return fileNode;
}


tError watchDirectory( tWatchedTree * watchedTree, const char * fullPath )
{
    tWatchID watchID = inotify_add_watch( watchedTree->inotify.fd, fullPath, IN_ALL_EVENTS );
    if (watchID == -1)
    {
        logError( "error watching directory \'%s\'", fullPath );
        return -errno;
    }
    else
    {
        tFSNode * fsNode;
        HASH_FIND( watchh, watchedTree->watchHashes, &watchID, sizeof(tWatchID), fsNode);

        if (fsNode == NULL)
        {
            fsNode = calloc(1, sizeof(tFSNode));
            if (fsNode != NULL)
            {
                fsNode->type = kDirectory;
                fsNode->watchID = watchID;
                fsNode->path = strdup( fullPath );
                HASH_ADD( watchh, watchedTree->watchHashes, watchID, sizeof( tWatchID ), fsNode );
                fsNode->pathHash = calcHash( fsNode->path );
                HASH_ADD( pathh, watchedTree->pathHashes, pathHash, sizeof( tHash ), fsNode );
            }
            logDebug( "new dir: ");
        }
        if ( fsNode != NULL )
        {
            displayFsNode( watchedTree, fsNode );
        }
    }
    return 0;
}

/**
 * @brief node is disappearing, so remove it from our structures, too.
 * @param watchedTree
 * @param fsNode
 */

void removeNode( tWatchedTree * watchedTree, tFSNode * fsNode )
{
    if (fsNode != NULL)
    {
        logDebug( "remove [%02d] \'%s\'", fsNode->watchID, fsNode->path );

        switch (fsNode->type)
        {
        case kDirectory:
            if ( watchedTree->watchHashes != NULL )
            {
                HASH_DELETE( watchh, watchedTree->watchHashes, fsNode );
            }
            break;

        case kFile:
            if ( watchedTree->pathHashes != NULL )
            {
                HASH_DELETE( pathh, watchedTree->pathHashes, fsNode );
            }
            const char * relPath = toRelativePath( watchedTree, fsNode->path );
            if ( relPath != NULL )
            {
                /* if a file is created and then deleted before it's ever processed, then
                 * it won't be present in the .seen hierarchy yet - and that's normal */
                if ( unlinkat( watchedTree->seen.fd, relPath, 0 ) == -1 && errno != ENOENT)
                {
                    logError( "failed to delete \'%s\'", fsNode->path );
                }
                errno = 0;
            }
            break;
        }
        if ( watchedTree->expiringList != NULL )
        {
            LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
        }

        free((void *)fsNode->path);
        free(fsNode);
    }
}


void ignoreNode( tWatchedTree * watchedTree, tWatchID watchID )
{
    /* the inotify ID has already been removed, and we won't be
     * seeing it again. so clean up our parallel structures */
    tFSNode * fsNode;

    HASH_FIND( watchh, watchedTree->watchHashes, &watchID, sizeof(tWatchID), fsNode);
    if ( fsNode != NULL)
    {
        logDebug( "ignore [%d]", watchID);
        removeNode( watchedTree, fsNode );
    }
}


tError processEvent( tWatchedTree * watchedTree, const struct inotify_event * event )
{
    int result = 0;

    tFSNode * watchedNode;
    HASH_FIND( watchh, watchedTree->watchHashes, &event->wd, sizeof( tWatchID ), watchedNode );
    if ( watchedNode != NULL )
    {
        displayEvent( watchedTree, watchedNode, event );
        /* Caution: if event->len is zero, the string at event->name may not be a valid C string!
         * If the event has no 'name', there's literally no string, NOT even an empty one.
         * event->name could be pointing at the beginning of the next event, or even after
         * the end of the buffer if it's the last event in it.
         */
        const char * fullPath = watchedNode->path;
        if (event->len > 0)
        {
            fullPath = catPath( watchedNode->path, event->name );
        }
        tHash hash = calcHash( fullPath );

        time_t expires = time(NULL) + 10;
        tFSNode * pathNode;
        HASH_FIND( pathh, watchedTree->pathHashes, &hash, sizeof( tHash ), pathNode );
        if ( pathNode == NULL && !(event->mask & IN_ISDIR))
        {
            // didn't find the hashed full path in this watchedTree, so create it
            struct stat fileInfo;
            if ( stat( fullPath, &fileInfo ) == -1 && errno != ENOENT)
            {
                logError( "unable to get information about \'%s\'", fullPath );
            }
            else
            {
                errno = 0;
                /* ToDo: check .seen instead of using link count */
                pathNode = makeFileNode( watchedTree, fullPath, kNew,
                                         (fileInfo.st_nlink == 1) ? expires : 0 );
            }
        }

        if ( pathNode != NULL && watchedNode != pathNode )
        {
            errno = 0;
            char nodeStr[PATH_MAX];
            fsNodeAsStr( nodeStr, sizeof(nodeStr), watchedTree, pathNode );
            logDebug( "%57c pathNode: %s", ' ', nodeStr );
        }

        if ( event->mask & IN_Q_OVERFLOW )
        {
            // checkRescans immediately
            watchedTree->nextRescan = time(NULL);
        }

        if ( event->mask & IN_DELETE )
        {
            if (pathNode != NULL)
            {
                removeNode( watchedTree, pathNode );
                if ( pathNode->type == kFile )
                {
                    const char * relPath = toRelativePath( watchedTree, fullPath);
                    if ( unlinkat( watchedTree->seen.fd, relPath, 0 ) == -1)
                    {
                        logError( "Error: unable to delete the hard-linked"
                                  " file in the 'seen' shadow hierarchy\n" );
                    }
                }
            }
        }
        else if ( event->mask & IN_CREATE )
        {
            if ( event->mask & IN_ISDIR )
            {
                watchDirectory( watchedTree, fullPath );
            }
            else
            {
                /* we already made a new fileNode, if it didn't already exist */
                setExpiration( watchedTree, pathNode, kNew, expires );
            }
        }
        else if ( event->mask & IN_CLOSE_WRITE)
        {
            tExpiredAction action = pathNode->expiredAction;
            if ( action != kNew )
            {
                action = kModified;
            }
            setExpiration( watchedTree, pathNode, action, expires );
        }
        else if (event->mask & (IN_MOVED_FROM | IN_MOVED_TO) )
        {
            /* The IN_MOVED_FROM and IN_MOVED_TO are issued in pairs,
             * tied together with the same (non-zero) cookie value  */
            if ( event->cookie != 0 )
            {
                tFSNode * cookieNode;
                HASH_FIND( cookieh, watchedTree->cookieHashes, &event->cookie, sizeof( tCookie ), cookieNode );
                if (cookieNode == NULL)
                {
                    cookieNode = pathNode;
                    cookieNode->cookie = event->cookie;
                    HASH_ADD( cookieh, watchedTree->cookieHashes, cookie, sizeof( tCookie ), cookieNode );
                }

                // occurs first of the pair - figure out the existing pathNode
                if ( event->mask & IN_MOVED_FROM )
                {
                    logDebug( "{%u} move [%02d] from \'%s\' #%lu",
                              cookieNode->cookie, cookieNode->watchID, cookieNode->path, cookieNode->pathHash );
                }

                // occurs second of the pair - update the root.path in the cookieNode identified in the IN_MOVED_FROM
                if ( event->mask & IN_MOVED_TO )
                {
                    if (cookieNode != NULL)
                    {
                        HASH_DELETE( pathh, watchedTree->pathHashes, cookieNode );
                        free( (char *)cookieNode->path );
                        cookieNode->path = strdup( fullPath );
                        cookieNode->pathHash = hash;
                        HASH_ADD( pathh, watchedTree->pathHashes, pathHash, sizeof(tHash), cookieNode );

                        HASH_DELETE( cookieh, watchedTree->cookieHashes, cookieNode );
                        cookieNode->cookie = 0;

                        setExpiration( watchedTree, cookieNode, kMoved, expires );

                        logDebug( "{%u} move [%02d] to \'%s\'",
                                 cookieNode->cookie, cookieNode->watchID, cookieNode->path );
                    }
                }
            }
        }
        else /* all other events */
        {
            if ( pathNode != NULL && pathNode->expires != 0)
            {
                setExpiration( watchedTree, pathNode, pathNode->expiredAction, time(NULL) + 10);
                logDebug( "deferred %s expiration of \'%s\'",
                           expiredActionAsStr[pathNode->expiredAction], pathNode->path );
            }
        }

        if (event->len > 0)
        {
            free( (char *)fullPath );
        }
    }

    /* for whatever reason, the watchID for this node will be invalid
     * after this event, so clean up any references we have to it */
    if ( event->mask & IN_IGNORED )
    {
        ignoreNode( watchedTree, event->wd );
    }

    return result;
}


tError processFile( tExpiredAction action, const char * path )
{
    tError    result = 0;
    tExecMsg  message;

    logDebug( "expired %s \'%s\'",
             expiredActionAsStr[ action ], path );
    message.action = action;
    strncpy( (char *)message.path, path, sizeof( message.path) );

    ssize_t len = write( g.pipe[kPipeWriteFD], &message, sizeof(tExpiredAction) +strlen(message.path) + 1 );
    if ( len == -1 )
    {
        logError( "writing to the pipe failed" );
        result = -errno;
    }
    return result;
}


tError processExpired( void )
{
    int result = 0;
     for ( tWatchedTree * watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {

        long now = time(NULL);
        for ( tFSNode * fsNode = watchedTree->expiringList;
              fsNode != NULL && fsNode->expires <= now;
              fsNode = fsNode->expiringNext )
        {
            result = processFile( fsNode->expiredAction, fsNode->path );
            if ( result == 0 )
            {
                /* processed sucessfully so take it off the list */
                LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
                fsNode->expires = 0;

                /* leave a persistent marker to remember this file has been seen before when rescanning */
                const char * relPath = toRelativePath( watchedTree, fsNode->path );
                tFileDscr fd = openat( watchedTree->seen.fd,
                                       relPath,
                                       O_WRONLY | O_CREAT,
                                       S_IRUSR | S_IWUSR );
                if ( fd == -1 )
                {
                    logError( "unable to mark %s as seen", fsNode->path );
                    result = -errno;
                }
                else
                {
                    close( fd );
                }
            }
        }
    }
    return result;
}


int watchOrphan( const char *fullPath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
    (void)sb;
    int result = FTW_CONTINUE;
    const char * base = &fullPath[ ftwbuf->base ];
    tWatchedTree * watchedTree = g.watchedTree;

    if ( watchedTree != NULL )
    {
        const char * relPath = toRelativePath( watchedTree, fullPath);

        switch ( typeflag )
        {
        case FTW_F: // fullPath is a regular file.
            if ( *base != '.')
            {
                logDebug( "%d%-*c file: %s", ftwbuf->level, ftwbuf->level * 4, ':', relPath );
                struct stat fileInfo;
                errno = 0;
                if ( fstatat( watchedTree->seen.fd, relPath, &fileInfo, 0  ) == -1 && errno != ENOENT )
                {
                    logError( "Failed to get info about shadow file \'%s\'", relPath );
                }
                else if ( errno == ENOENT )
                {
                    makeFileNode( watchedTree, fullPath, kNew, time(NULL) + 10 );
                }
            }
            break;

        case FTW_D: // fullPath is a directory.
            if ( base[0] == '.' || strcmp( fullPath, watchedTree->seen.path ) == 0 )
            {
                result = FTW_SKIP_SUBTREE;
            }
            else
            {
                logDebug( "%d%-*c  dir: %s",
                          ftwbuf->level, ftwbuf->level * 4, ':', relPath );
                /* make sure that the corresponding shadow directory exists */
                if ( strlen( relPath ) > 0 )
                {
                    if ( mkdirat( watchedTree->seen.fd, relPath, S_IRWXU) == -1
                        && errno != EEXIST )
                    {
                        logError( "Unable to create directory %s{%s}",
                                   watchedTree->seen.path, relPath );
                    }
                    errno = 0;
                }

                watchDirectory( watchedTree, fullPath );
            }
            break;

        default:
            logError( "Error: %d: typeflag %d for %s", ftwbuf->level, typeflag, fullPath );
            break;
        }
    }
    return result;
}

tError scanTree( tWatchedTree * watchedTree )
{
    int result = 0;

    g.watchedTree = watchedTree;

    /* scan for files we have not seen before, and set them up to expire (and subsequently processed) */
    result = nftw( watchedTree->root.path, watchOrphan, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
    if ( result != 0 )
    {
        logError( "Error: couldn't watch new files in the \'%s\' directory", watchedTree->root.path );
        result = -errno;
    }

    g.watchedTree = NULL;

    watchedTree->nextRescan = time(NULL) + 60;

    return result;
}

time_t nextExpiration( void )
{
    time_t result;

    const char * whatExpires = NULL;
    tExpiredAction whyExpires = kRescan;
    time_t whenExpires = (unsigned)(-1L); // the end of time...
    for ( tWatchedTree * watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        if ( watchedTree->nextRescan < whenExpires )
        {
            whenExpires = watchedTree->nextRescan;
            whyExpires  = kRescan;
            whatExpires = "rescan";
        }
        /* head of the list should be the next to expire */
        tFSNode * pathNode = watchedTree->expiringList;
        if (pathNode != NULL )
        {
            if ( pathNode->expires != 0 && whenExpires > pathNode->expires)
            {
                whenExpires = pathNode->expires;
                whyExpires  = pathNode->expiredAction;
                whatExpires = pathNode->path;
            }
        }
    }

    result = whenExpires - time(NULL);
    logDebug( "%s expires %s in %ld seconds",
             whatExpires, expiredActionAsStr[whyExpires], result );
    if (result < 0)
        result = 1;

    return result;
}


tError checkRescans( void )
{
    tError result = 0;

    for ( tWatchedTree * watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        /* re-scan the hierarchy periodically, and when forced */
        if ( time(NULL) >= watchedTree->nextRescan )
        {
            result = scanTree( watchedTree );
            logDebug( "next periodic in %ld secs",
                     watchedTree->nextRescan - time(NULL) );
        }
    }
    return result;
}


tError scanLoop( void )
{
    int result = 0;
    struct epoll_event epollEvents[32];

    g.running = true;
    do {
        time_t secsUntil = nextExpiration();

        /* * * * block waiting for events or a timeout * * * */
        int count = epoll_wait( g.epollfd,
                                epollEvents,
                                sizeof(epollEvents)/sizeof(struct epoll_event),
                                secsUntil * 1000 );
        /* * * * * * * * * * * * * * * * * * * * * * * * * * */

        if ( count < 0 )
        {
            switch ( errno )
            {
            case EINTR:
            case EAGAIN:
                break;

            default:
                logError( "epoll() returned %d", count );
                break;
            }
        }
        else if ( count > 0 )
        {
            ssize_t len;

            /* Loop over the epoll events we just read from the epoll file descriptor. */
            for ( int i = 0; i < count && result == 0; ++i )
            {
                if ( epollEvents[ i ].events & EPOLLIN )
                {
                    /* Some systems cannot read integer variables if they are not properly aligned.
                     * On other systems, incorrect alignment may decrease performance.
                     * Hence, the buffer used for reading from the inotify.fd file descriptor should
                     * have the same alignment as struct inotify_event.
                     */

                    byte buf[4096]
                        __attribute__ ((aligned(__alignof__(struct inotify_event))));

                    tWatchedTree * watchedTree = epollEvents[ i ].data.ptr;

                    len = read( watchedTree->inotify.fd, buf, sizeof( buf ));
                    /* If the nonblocking read() found no events to read, then it returns
                     * -1 with errno set to EAGAIN. In that case, we exit the loop. */

                    if ( len < 0 && errno != EAGAIN )
                    {
                        logError( "unable to read from iNotify fd" );
                        result = -errno;
                        break;
                    }
                    errno = 0;
                    /* Loop over all iNotify events in the buffer. */
                    for ( const byte * event = buf;
                          event < buf + len;
                          event += sizeof( struct inotify_event ) + ((const struct inotify_event *)event)->len )
                    {
                        result = processEvent( watchedTree, (const struct inotify_event *)event );
                    }
                }
            }
        }

        if (result == 0)
        {
            result = processExpired();
        }

        if (result == 0)
        {
            result = checkRescans();
        }

    } while ( result == 0 && g.running == true );
    logInfo( "loop terminated %d", result );
    return result;
}

/********************************************/

/**
 * @brief normalize the root.path to be a full absolute root.path, and validate it
 * @param path
 * @return 0 on success, -errno on failure
 */
const char * normalizePath( const char * path)
{
    const char * result;
    struct stat info;
    result = realpath( path, NULL );
    if (result == NULL )
    {
        logError( "Couldn't convert \'%s\' into an absolute root.path", path );
    } else {
        if ( stat( result, &info ) < 0)
        {
            logError( "Couldn't get info about \'%s\'", result );
            result = NULL;
        } else {
            if ( ! S_ISDIR(info.st_mode) )
            {
                errno = ENOTDIR;
                logError( "\'%s\' is not a directory", result );
                result = NULL;
            } else {
                if ( access( result, W_OK ) == -1 )
                {
                    logError( "Cannot write to directory \'%s\'", result );
                    result = NULL;
                }
            }
        }
    }
    return result;
}


tWatchedTree * createTree( const char * dir )
{
    int result = 0;

    tWatchedTree * watchedTree;

    logDebug( " processing tree \'%s\'", dir );

    watchedTree = calloc( 1, sizeof(tWatchedTree) );
    if ( watchedTree != NULL )
    {
        watchedTree->root.path = normalizePath( dir );
        if ( watchedTree->root.path == NULL )
        {
            logError( "unable to normalize the root.path \'%s\'", dir );
        }
        else
        {
            logDebug( " absolute root.path \'%s\'", watchedTree->root.path );
            watchedTree->root.pathLen = strlen( watchedTree->root.path );
            watchedTree->root.fd = open( watchedTree->root.path, O_DIRECTORY );
            if ( watchedTree->root.fd < 0 )
            {
                logError( "couldn't open the \'%s\' directory",
                         dir );
                result = -errno;
            } else
            {
                result = makeSeenDir( watchedTree );
            }
        }
        logDebug( "root.fd: %d, seen.fd: %d", watchedTree->root.fd, watchedTree->seen.fd );

        // structure was calloc'd, so unnecessary to set pointers to NULL
        // watchedTree->watchHashes  = NULL;
        // watchedTree->pathHashes   = NULL;
        // watchedTree->cookieHashes = NULL;
        // watchedTree->expiringList = NULL;

        watchedTree->nextRescan = time(NULL) + 60;

        watchedTree->inotify.fd  = inotify_init();
        if ( watchedTree->inotify.fd == -1 )
        {
            logError( "Unable to register for filesystem events" );
            result = -errno;
        }
        else
        {
            struct epoll_event epollEvent;
            epollEvent.events   = EPOLLIN;
            epollEvent.data.ptr = watchedTree;

            if ( epoll_ctl( g.epollfd, EPOLL_CTL_ADD, watchedTree->inotify.fd, &epollEvent ) == -1 )
            {
                logError( "unable to register inotify.fd fd %d with epoll fd %d",
                          watchedTree->inotify.fd, g.epollfd );
                result = -errno;
            }
        }

        if ( result != 0 )
        {
            free( watchedTree );
            watchedTree = NULL;
        }
        else
        {
            watchedTree->next = g.treeList;
            g.treeList        = watchedTree;
        }
    }

    return watchedTree;
}

tError initPidFilename( const char * executableName )
{
    tError result = 0;
    errno = 0;
    if ( g.pidFilename == NULL)
    {
        size_t len = sizeof( "/var/run//.pid" ) + strlen( executableName ) * 2;
        char * pidName = calloc( 1, len );
        if ( pidName == NULL)
        {
            logError( "Error: unable to allocate memory  " );
            result = -errno;
        }
        else
        {
            snprintf( pidName, len, "/var/run/%s", executableName );
            if ( mkdir( pidName, S_IRWXU) == -1 && errno != EEXIST )
            {
                logError( "Error: unable to create directory %s",
                          pidName );
                result = -errno;
            }
            else
            {
                errno = 0;
                snprintf( pidName, len, "/var/run/%s/%s.pid", executableName, executableName );
                g.pidFilename = pidName;
            }
        }
    }
    return result;
}

tError createPidFile( pid_t pid )
{
    tError result = 0;

    if ( g.pidFilename != NULL )
    {
        FILE * pidFile = fopen( g.pidFilename, "w" );
        if ( pidFile == NULL)
        {
            logError( "Error: unable to open %s for writing",
                     g.pidFilename );
            result = -errno;
        }
        else {
            if ( fprintf( pidFile, "%d", pid ) == -1 )
            {
                logError( "Error: unable to write pid to %s",
                         g.pidFilename );
                result = -errno;
            }
            fclose( pidFile );
        }
    }
    return result;
}

pid_t getDaemonPID( void )
{
    pid_t result;

    FILE * pidFile = fopen( g.pidFilename, "r" );
    if ( pidFile == NULL )
    {
        logError( "Error: unable to open %s for reading",
                 g.pidFilename );
        result = -errno;
    }
    else {
        if ( fscanf( pidFile, "%d", &result ) == -1 || result == 0 )
        {
            logError( "Error: unable to read pid from %s",
                      g.pidFilename );
            result = -errno;
        }
        fclose( pidFile );
    }

    return result;
}

void daemonExit(void)
{
    logError( "daemonExit()" );
    if (g.pidFilename != NULL)
    {
        unlink( g.pidFilename );
        free( (char *)g.pidFilename );
        g.pidFilename = NULL;
    }
    g.running = false;
}

void daemonSignal(int signum)
{
    logError( "daemonSignal(%d)", signum );
    daemonExit();
}


tError executeLoop( void )
{
    tError result = 0;
    struct pollfd fds[1];

    fds[0].fd = g.pipe[ kPipeReadFD ];
    fds[0].events = POLLIN;
    do {
        int count = poll( fds, 1, -1 );
        if ( count < 0 )
        {
            logError( "Poll failed");
            result = -errno;
        }
        else if ( (fds[0].revents & POLLIN) && count > 0 )
        {
            tExecMsg message;
            errno = 0;
            ssize_t len = read( g.pipe[kPipeReadFD], &message, sizeof(message) );
            if ( len < 0 )
            {
                logError( "read failed" );
                result = -errno;
            }
            else if ( (size_t)len > sizeof( tExpiredAction ) + 2 ) //smallest possible valid message
            {
                logDebug( ">>> execute action: %s path: \'%s\'", expiredActionAsStr[message.action], message.path );
            }
        }
    } while (result == 0);

    return result;
}

tError startExecution( void )
{
    int result = 0;
    if ( pipe2( g.pipe, O_DIRECT ) == -1 )
    {
        logError( "Unable to set up pipe" );
    } else
    {
        pid_t pid = fork();
        switch (pid)
        {
        case -1: // fork failed
            logError( "Unable to fork" );
            result = -errno;
            break;

        case 0: // child
            if ( close( g.pipe[ kPipeWriteFD ] ) == -1 )
            {
                logError( "unable to close unused file descriptor at child end of pipe" );
                result = -errno;
            }
            else
            {
                result = executeLoop();
            }
            break;

        default: // parent
            if ( close( g.pipe[ kPipeReadFD ] ) == -1 )
            {
                logError( "unable to close unused file descriptor at parent end of pipe" );
                result = -errno;
            }
            else
            {
                result = scanLoop();
            }
            break;
        }
    }
    return result;
}

/**
 * @brief
 */
tError startDaemon(void)
{
    tError  result = 0;
    pid_t   pid = fork();
    switch (pid)
    {
    case -1: // failed
        logError( "Unable to start daemon process" );
        result = -errno;
        break;

    case 0: // child
        /* detach from parent, and become an independent process group leader */
        pid = setsid();

        result = createPidFile( pid );
        if ( result == 0 )
        {
            /* register a handler for SIGTERM to remove the pid file */
            struct sigaction new_action, old_action;

            sigemptyset( &new_action.sa_mask );
            new_action.sa_handler = daemonSignal;
            new_action.sa_flags = 0;

            sigaction( SIGTERM, &new_action, &old_action );

            /* and if we exit normally, also remove the pid file */
            if ( atexit( daemonExit ) != 0 )
            {
                logError( "Error: daemon failed to register an exit handler" );
                result = -errno;
            }
        }
        if ( result == 0 )
        {
            result = startExecution();
            // startExecution() is not expected to return in normal operation
        }
        return result;

    default: // parent
        // everything happens in the child
        break;
    }
    return result;
}

/* Note: this will probably be called from another isntance invoked with --kill */
tError stopDaemon( void )
{
    tError  result = 0;
    pid_t   pid = getDaemonPID();
    logError( "pid: \'%d\'",
              pid );
    if ( pid < 0 )
    {
        result = pid;
    }
    else if ( killpg( pid, SIGTERM ) == -1 )
    {
        logError( "Error: failed to signal the daemon" );
        result = -errno;
    }
    return result;
}

tError processConfig( int argc, char * argv[] )
{
    tError result = 0;
    char path[PATH_MAX];

    /* the global arg_xxx structs above are initialised within the argtable */
    void * argtable[] =
     {
         g.option.help        = arg_lit0( "h", "help",
                                          "display this help (and exit)" ),
         g.option.version     = arg_lit0( "V", "version",
                                          "display version info (and exit)" ),
         g.option.killDaemon  = arg_lit0( "k", "kill",
                                          "shut down the background daemon" ),
         g.option.foreground  = arg_lit0( "f", "foreground",
                                          "stay in the foregrounnd (don't daemonize)" ),
         g.option.debugLevel  = arg_int0( "d", "debug-level", "",
                                          "set the level of detail being logged (0-7, 0 is least detailed)" ),
         g.option.path        = arg_filen(NULL, NULL, "<file>", 0, 20,
                                          "input files" ),

         g.option.end         = arg_end( 20 )
     };

    result = initPidFilename( g.executableName );
    logDebug( "pidFilename: \'%s\'", g.pidFilename );

    config_init( g.config );

    snprintf( path, sizeof(path), "/etc/%s.conf", g.executableName );
    if ( access( path, R_OK) == -1 )
    {
        logWarning( " unabled to read %s", path );
    }
    else
    {
        config_read_file( g.config, path );
    }

    result = arg_parse( argc, argv, argtable );

    if ( result > 0 )    /* If the parser returned any errors then display them and exit */
    {
        /* Display the error details contained in the arg_end struct.*/
        arg_print_errors( stdout, g.option.end, g.executableName );
        fprintf( stdout, "Try '%s --help' for more information.\n", g.executableName );
        result = -EINVAL;
    }
    else if ( g.option.help->count > 0 )    /* special case: '--help' takes precedence over everything else */
    {
        fprintf( stdout, "Usage: %s", g.executableName );
        arg_print_syntax( stdout, argtable, "\n" );
        fprintf( stdout, "process watchHashes file into a header file.\n\n" );
        arg_print_glossary( stdout, argtable, "  %-25s %s\n" );
        fprintf( stdout, "\n" );

        result = 0;
    }
    else if ( g.option.version->count > 0 )   /* ditto for '--version' */
    {
        fprintf( stdout, "%s, version %s\n", g.executableName, "(to do)" );
    }
    else if ( g.option.killDaemon->count > 0 )   /* and for '--kill' */
    {
        stopDaemon( );
    }
    else {
        g.epollfd = epoll_create1( 0 );

        if ( result == 0 )
        {
            tWatchedTree * watchedTree;
            for ( int i = 0; i < g.option.path->count && result == 0; i++ )
            {
                watchedTree = createTree( g.option.path->filename[ i ] );
                result      = scanTree( watchedTree );
            }
        }
        if ( result == 0 )
        {
            result = startDaemon();
        }
    }

    /* release each non-null entry in argtable[] */
    arg_freetable( argtable, sizeof( argtable ) / sizeof( argtable[ 0 ] ));

    return result;
}

tError main( int argc, char * argv[] )
{
    int result = 0;


    g.executableName = strrchr( argv[0], '/' );
    /* If we found a slash, increment past it. If there's no slash, point at the full argv[0] */
    if ( g.executableName++ == NULL)
    {
        g.executableName = argv[0];
    }
    initLogStuff( g.executableName );

    if (result == 0)
    {
        result = processConfig( argc, argv );
    }
    return result;
}
