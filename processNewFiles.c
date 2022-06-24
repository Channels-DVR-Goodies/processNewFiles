//
// Created by paul on 6/5/22.
//

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <stdbool.h>

#include <dlfcn.h>
#include <fcntl.h>

#include <argtable3.h>
#include <unistd.h>
#include <ftw.h>
#include <sys/inotify.h>

#include <sys/epoll.h>

#include <uthash.h>
#include <utlist.h>

#undef  QUEUE_SUPPORT

typedef unsigned char byte;
typedef unsigned long tHash;
typedef uint32_t      tCookie;
typedef int           tWatchID;
typedef int           tFileDscr;
typedef int           tError;

typedef enum { kRescan=0, kExists, kCreated, kClosedAfterWrite, kMovedOut, kMoved } tExpiredAction;

const char * expiredActionAsStr[] = {
    [kRescan]           = "rescan",
    [kExists]           = "exists",
    [kCreated]          = "created",
    [kClosedAfterWrite] = "closed",
    [kMovedOut]         = "moved-out",
    [kMoved]            = "moved"
};

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
    int             debugLevel;

    int             epollfd;
    tWatchedTree *  treeList;
    tWatchedTree *  watchedTree;

    struct
    {
        struct arg_lit  * help;
        struct arg_lit  * version;
        struct arg_lit  * zero;
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


int appendStr( char * buffer, const char ** separator, const char * string, int remaining )
{
    if (remaining > 0)
    {
        strncat( buffer, *separator, remaining );
        remaining -= strlen( *separator );
        if ( remaining < 0 ) remaining = 0;
    }

    if (remaining > 0)
    {
        strncat(buffer, string, remaining );
        remaining -= strlen( string );
        if (remaining < 0 ) remaining = 0;
    }
    *separator = " | ";

    return (size_t) remaining;
}

const char * toRelativePath( tWatchedTree * watchedTree, const char * fullPath )
{
    const char * result = &fullPath[ watchedTree->root.pathLen ];
    if ( *result == '/' ) ++result;
    return result;
}

int orderedByExpiration( tFSNode * newNode, tFSNode * existingNode )
{
    if ( newNode->expires < existingNode->expires ) return -1;
    if ( newNode->expires > existingNode->expires ) return 1;
    return 0;
}

void displayInotifyEventType(const struct inotify_event * event)
{
    char buffer[30];
    const char * separator = " ";
    int  remaining = sizeof(buffer) - 1;

    buffer[0] = '\0';

    /* Supported events suitable for MASK parameter of INOTIFY_ADD_WATCH.  */
    if ( event->mask & IN_ACCESS ) /* File was accessed.  */
        remaining = appendStr( buffer, &separator, "IN_ACCESS", remaining);
    if ( event->mask & IN_MODIFY ) /* File was modified.  */
    	remaining = appendStr( buffer, &separator, "IN_MODIFY", remaining );
    if ( event->mask & IN_ATTRIB ) /* Metadata changed.  */
    	remaining = appendStr( buffer, &separator, "IN_ATTRIB", remaining );
    if ( event->mask & IN_CLOSE_WRITE ) /* Writtable file was closed.  */
    	remaining = appendStr( buffer, &separator, "IN_CLOSE_WRITE", remaining );
    if ( event->mask & IN_CLOSE_NOWRITE ) /* Unwrittable file closed.  */
    	remaining = appendStr( buffer, &separator, "IN_CLOSE_NOWRITE", remaining );
    if ( event->mask & IN_OPEN ) /* File was opened.  */
    	remaining = appendStr( buffer, &separator, "IN_OPEN", remaining );
    if ( event->mask & IN_MOVED_FROM ) /* File was moved from X.  */
    	remaining = appendStr( buffer, &separator, "IN_MOVED_FROM", remaining );
    if ( event->mask & IN_MOVED_TO ) /* File was moved to Y.  */
    	remaining = appendStr( buffer, &separator, "IN_MOVED_TO", remaining );
    if ( event->mask & IN_CREATE ) /* Subfile was created.  */
    	remaining = appendStr( buffer, &separator, "IN_CREATE", remaining );
    if ( event->mask & IN_DELETE ) /* Subfile was deleted.  */
    	remaining = appendStr( buffer, &separator, "IN_DELETE", remaining );
    if ( event->mask & IN_DELETE_SELF ) /* Self was deleted.  */
    	remaining = appendStr( buffer, &separator, "IN_DELETE_SELF", remaining );
    if ( event->mask & IN_MOVE_SELF ) /* Self was moved.  */
    	remaining = appendStr( buffer, &separator, "IN_MOVE_SELF", remaining );

    /* Events sent by the kernel.  */
    if ( event->mask & IN_UNMOUNT ) /* Backing fs was unmounted.  */
    	remaining = appendStr( buffer, &separator, "IN_UNMOUNT", remaining );
    if ( event->mask & IN_Q_OVERFLOW ) /* Event queued overflowed.  */
    	remaining = appendStr( buffer, &separator, "IN_Q_OVERFLOW", remaining );
    if ( event->mask & IN_IGNORED ) /* File was ignored.  */
    	remaining = appendStr( buffer, &separator, "IN_IGNORED", remaining );
    if ( event->mask & IN_ISDIR ) /* refers to a directory */
        remaining = appendStr( buffer, &separator, "IN_ISDIR", remaining );

    fprintf( stderr, "%-*s", (int)sizeof(buffer), buffer );
}


void displayFsNode( tWatchedTree * watchedTree, tFSNode * fsNode )
{
    /* Print the name of the watched directory. */
    if ( fsNode != NULL)
    {
        fprintf( stderr, "[%02d] \'{%.*s}%s\'",
                 fsNode->watchID,
                 (int)watchedTree->root.pathLen,
                 fsNode->path,
                 &fsNode->path[ watchedTree->root.pathLen ]);
        if ( fsNode->cookie != 0 )
        {
            fprintf( stderr, " {%u}", fsNode->cookie);
        }
        if ( fsNode->expires != 0 )
        {
            fprintf( stderr, " expires %s in %lu",
                     expiredActionAsStr[fsNode->expiredAction],
                     fsNode->expires - time(NULL) );
        }
        fputc( '\n', stderr);
    }
}


void displayEvent( tWatchedTree * watchedTree, const struct inotify_event * event )
{
    /* display the inotify.fd descriptor */
    if (event != NULL)
    {
        fprintf( stderr, "[%02u] ", event->wd );

        /* display event type. */
        displayInotifyEventType( event );

        if ( event->cookie != 0 )
            fprintf( stderr, " (%u)", event->cookie );


        /* Print the type of the event */
        /* Print the name of the file. */
        if ( event->len )
        {
            int l = 18 - strlen( event->name );
            if ( l < 0 )
            {
                l = 0;
            }
            fprintf( stderr, "\'%s\'%*c", event->name, l, ' ' );
        }
        else
        {
            fprintf( stderr, "%20c", ' ' );
        }
        if (watchedTree == NULL) fputc('\n', stderr);
        else
        {
            tFSNode * fsNode;
            HASH_FIND( watchh, watchedTree->watchHashes, &event->wd, sizeof(tWatchID), fsNode);
            if ( fsNode == NULL)
            {
                fprintf( stderr, "Error: watchID [%02d] not found\n", event->wd );
            }
        }
    }
}


void displayHash( tWatchedTree * watchedTree )
{
    int count = HASH_CNT( watchh,watchedTree->watchHashes );
    fprintf( stderr, "count: %d\n", count );

    for ( tFSNode * wn = watchedTree->watchHashes; wn != NULL; wn = wn->watchh.next )
    {
        displayFsNode( watchedTree, wn );
    }
}


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
        fprintf( stderr, "Error: couldn't create subdirectory \'%s\' (%d: %s)\n",
                 watchedTree->seen.path, errno, strerror(errno));
        result = -errno;
    } else {
        watchedTree->seen.fd = openat( watchedTree->root.fd, subdir, O_DIRECTORY );
        if ( watchedTree->seen.fd < 0 )
        {
            fprintf( stderr, "Error: couldn't open subdirectory \'%s\' (%d: %s)\n",
                     watchedTree->seen.path, errno, strerror(errno));
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
    if ( fileNode != NULL)
    {
        fprintf( stderr, "existing fileNode: ");
    }
    else {
        // didn't find the hash - must be a new one, so make a matching fsNode
        fileNode = calloc( 1, sizeof(tFSNode));
        if (fileNode != NULL)
        {
            fileNode->type      = kFile;
            fileNode->path      = strdup( fullPath );
            fileNode->pathHash  = calcHash( fileNode->path );
            HASH_ADD( pathh, watchedTree->pathHashes, pathHash, sizeof(tHash), fileNode );

            fprintf( stderr, "new fileNode: ");
        }
    }
    if (fileNode != NULL)
    {
        setExpiration( watchedTree, fileNode, action, expires );
        displayFsNode( watchedTree, fileNode );
    }
    return fileNode;
}


tError watchDirectory( tWatchedTree * watchedTree, const char * fullPath )
{
    tWatchID watchID = inotify_add_watch( watchedTree->inotify.fd, fullPath, IN_ALL_EVENTS );
    if (watchID == -1)
    {
        fprintf( stderr, "error watching directory %s (%d: %s)\n",
                 fullPath, errno, strerror(errno) );
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
            fprintf( stderr, "new watchID: ");
        } else {
            fprintf( stderr, "existing watchID: ");
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
        fprintf( stderr, "remove [%02d] \'%s\'\n", fsNode->watchID, fsNode->path );
        if ( (fsNode->type == kDirectory) && (watchedTree->watchHashes != NULL) )
            HASH_DELETE( watchh, watchedTree->watchHashes, fsNode );
        if ( fsNode->type == kFile && watchedTree->pathHashes != NULL )
            HASH_DELETE( pathh, watchedTree->pathHashes, fsNode );
        if ( watchedTree->expiringList != NULL )
            LL_DELETE2( watchedTree->expiringList, fsNode, expiringNext );
        if ( fsNode->type == kFile )
        {
            const char * relPath = toRelativePath( watchedTree, fsNode->path );
            if ( relPath != NULL )
            {
                /* if a file is created and then deleted before it's ever processed, then
                 * there won't be a hard link in the .seen hierarchy - and that's normal */
                if ( unlinkat( watchedTree->seen.fd, relPath, 0 ) == -1 && errno != ENOENT)
                {
                    fprintf( stderr, "failed to delete %s (%d: %s)\n",
                             fsNode->path, errno, strerror(errno) );
                }
            }
        }
        free((char *)fsNode->path);
        free(fsNode);
    }
}

void ignoreNode( tWatchedTree * watchedTree, tWatchID watchID )
{
    /* the inotify.fd ID has already been removed, and we won't be
     * seeing it again. so clean up our parallel structures */
    tFSNode * fsNode;

    HASH_FIND( watchh, watchedTree->watchHashes, &watchID, sizeof(tWatchID), fsNode);
    if ( fsNode != NULL)
    {
        fprintf( stderr, "ignore [%d] and ", watchID);
        removeNode( watchedTree, fsNode );
    }
}



tError processEvent( tWatchedTree * watchedTree, const struct inotify_event * event )
{
    int result = 0;

    displayEvent( watchedTree, event );

    tFSNode * watchedNode;
    HASH_FIND( watchh, watchedTree->watchHashes, &event->wd, sizeof( tWatchID ), watchedNode );
    if ( watchedNode != NULL )
    {
        displayFsNode( watchedTree, watchedNode );
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
            // didn't find the full root.path in this watchedTree, so create it
            struct stat fileInfo;
            if ( stat( fullPath, &fileInfo ) == -1)
            {
                fprintf( stderr, "unable to get information about \'%s\' (%d: %s)\n",
                         fullPath, errno, strerror(errno) );
            }
            else
            {
                pathNode = makeFileNode( watchedTree, fullPath, kExists,
                                         (fileInfo.st_nlink == 1) ? expires : 0 );
            }
        }
        if ( pathNode != NULL && watchedNode != pathNode )
        {
            fprintf( stderr, "%44c pathNode: ",' ' );
            displayFsNode( watchedTree, pathNode );
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
                        fprintf( stderr,
                                 "Error: unable to delete the hard-linked file in the 'seen' "
                                 "shadow hierarchy (%d: %s)\n", errno, strerror(errno) );
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
                /* update the expiration */
                setExpiration( watchedTree, pathNode, kCreated, expires );
            }
        }
        else if ( event->mask & IN_CLOSE_WRITE)
        {
            setExpiration( watchedTree, pathNode, kClosedAfterWrite, expires );
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
                }
                cookieNode->cookie = event->cookie;
                HASH_ADD( cookieh, watchedTree->cookieHashes, cookie, sizeof( tCookie ), cookieNode );

                // occurs first of the pair - figure out the existing pathNode
                if ( event->mask & IN_MOVED_FROM )
                {
                    // in case we don't get an IN_MOVED_TO event with the same cookie within 5 seconds
                    setExpiration( watchedTree, cookieNode, kMovedOut, time(NULL) + 5);
                    fprintf( stderr, "{%u} move [%02d] from \'%s\' #%lu\n",
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

                        fprintf( stderr, "{%u} move [%02d] to \'%s\'\n",
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
                fprintf( stderr, "deferred %s expiration of \'%s\'\n",
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
    fprintf( stderr, "expired %s \'%s\'\n",
             expiredActionAsStr[ action ], path );
    return 0;
}

tError processExpired( void )
{
    int result = 0;
    tWatchedTree * watchedTree;
    for ( watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {

        long now = time(NULL);
        for ( tFSNode * fsNode = watchedTree->expiringList;
              fsNode != NULL && fsNode->expires <= now;
              fsNode = fsNode->expiringNext )
        {
            result = processFile( fsNode->expiredAction, fsNode->path );
            setExpiration( watchedTree, fsNode, kExists, 0 );
            if ( result == 0 )
            {
                const char * relPath = toRelativePath( watchedTree, fsNode->path );
                if ( linkat( watchedTree->root.fd, relPath,
                             watchedTree->seen.fd, relPath, 0 ) == -1 )
                {
                    fprintf( stderr, "unable to mark %s as seen (%d: %s)\n",
                             fsNode->path, errno, strerror(errno));
                    result = -errno;
                }
            }
        }
    }
    return result;
}

/* note: only intended to be used on the 'seen' direcory, to clean up any leftovers */
int deleteOrphan( const char *fullPath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
    int result = FTW_CONTINUE;
    const char * base = &fullPath[ ftwbuf->base ];

    switch ( typeflag )
    {
    case FTW_F: // fullPath is a regular file.
        // fprintf( stderr, "deleteOrphan file: %s (%lu)\n", fullPath, sb->st_nlink );
        if ( sb->st_nlink == 1 )
        {
            fprintf( stderr, "delete orphan: %s\n", fullPath );
            unlink( fullPath );
        }
        break;

    case FTW_D: // fullPath is a directory.
        // fprintf( stderr, "deleteOrphan root.fd: %s\n", fullPath );

        if ( ftwbuf->level > 0 && base[0] == '.' )
        {
            result = FTW_SKIP_SUBTREE;
        }
        break;

    default:
        fprintf( stderr, "Error: %d: typeflag %d\n", ftwbuf->level, typeflag );
        break;
    }
    return result;
}


int watchOrphan( const char *fullPath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
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
                fprintf( stderr, "%d%-*c file: %s (%lu)\n",
                         ftwbuf->level, ftwbuf->level * 4, ':', relPath, sb->st_nlink );
                time_t expires = 0;
                if ( sb->st_nlink == 1 )
                {
                    expires = time(NULL) + 10;
                }
                makeFileNode( watchedTree, fullPath, kExists, expires );
            }
            break;

        case FTW_D: // fullPath is a directory.
            if ( base[0] == '.' )
            {
                result = FTW_SKIP_SUBTREE;
            }
            else {
                fprintf( stderr, "%d%-*c  dir: %s\n",
                         ftwbuf->level, ftwbuf->level * 4, ':', relPath );
                if ( strlen(relPath) > 0 )
                {
                    if ( mkdirat( watchedTree->seen.fd, relPath, S_IRWXU | S_IRWXG) == -1
                      && errno != EEXIST)
                    {
                        fprintf( stderr, "unable to create directory {%s}%s (%d: %s)\n",
                                 watchedTree->seen.path, relPath, errno, strerror(errno));
                    }
                }
                watchDirectory( watchedTree, fullPath );
            }
            break;

        default:
            fprintf( stderr, "Error: %d: typeflag %d\n", ftwbuf->level, typeflag );
            break;
        }
    }
    return result;
}

tError scanTree( tWatchedTree * watchedTree )
{
    int result = 0;

    g.watchedTree = watchedTree;

    // scan for orphans, and delete them
    result = nftw( watchedTree->seen.path, deleteOrphan, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
    if (result != 0)
    {
        fprintf( stderr, "Error: couldn't prune orphans in the \'%s\'  directory (%d: %s)",
                 watchedTree->seen.path, errno, strerror(errno) );
        result = -errno;
    } else {
        /* scan for files we have not seen before, and set them up to expire (and subsequently processed) */
        result = nftw( watchedTree->root.path, watchOrphan, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
        if (result != 0)
        {
            fprintf( stderr, "Error: couldn't watch new files in the \'%s\' directory (%d: %s)",
                     watchedTree->root.path, errno, strerror(errno) );
            result = -errno;
        }
    }
    g.watchedTree = NULL;

    watchedTree->nextRescan = time(NULL) + 60;

    return result;
}

time_t nextExpiration( void )
{
    time_t result;

    tWatchedTree * watchedTree;

    const char * whatExpires = NULL;
    tExpiredAction whyExpires = kRescan;
    time_t whenExpires = (unsigned)(-1L); // the end of time...
    for ( watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        if ( watchedTree->nextRescan < whenExpires )
        {
            whenExpires = watchedTree->nextRescan;
            whyExpires  = kRescan;
            whatExpires = "next";
        };
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
    fprintf( stderr, "%s expires %s in %ld seconds\n",
             whatExpires, expiredActionAsStr[whyExpires], result );
    if (result < 0)
        result = 1;

    return result;
}


tError checkRescans( void )
{
    tError result = 0;

    tWatchedTree * watchedTree;
    for ( watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        /* re-scan the hierarchy periodically, and when forced */
        if ( time(NULL) >= watchedTree->nextRescan )
        {
            result = scanTree( watchedTree );
            fprintf( stderr, "next periodic in %ld secs\n",
                     watchedTree->nextRescan - time(NULL) );
        }
    }
    return result;
}


tError scanLoop( void )
{
    int result = 0;
    struct epoll_event epollEvents[32];

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
            fprintf( stderr, "Error: poll( returned %d (%d: %s)\n", count, errno, strerror(errno) );
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
                        perror( "read" );
                        result = -errno;
                        break;
                    }
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

    } while ( result == 0 );
    fprintf( stderr, "loop terminated %d\n", result );
    return result;
}

/* ****************************************** *\

    one-time tree setup from this point on

\* ****************************************** */

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
        fprintf( stderr, "Error: couldn't convert \'%s\' into an absolute root.path (%d: %s)\n",
                 path, errno, strerror(errno));
    } else {
        if ( stat( result, &info ) < 0)
        {
            fprintf( stderr, "Error: couldn't get info about \'%s\' (%d: %s)\n",
                     result, errno, strerror(errno));
            result = NULL;
        } else {
            if ( ! S_ISDIR(info.st_mode) )
            {
                errno = ENOTDIR;
                fprintf( stderr, "Error: \'%s\' is not a directory (%d: %s)\n",
                         result, errno, strerror(errno) );
                result = NULL;
            } else {
                if ( access( result, W_OK ) == -1 )
                {
                    fprintf( stderr, "Error: cannot write to directory \'%s\' (%d: %s)\n",
                             result, errno, strerror(errno) );
                    result = NULL;
                }
            }
        }
    }
    return result;
}

#if 0
tWatchedTree * findTree( const char * dir )
{
    tWatchedTree * result = NULL;
    int longestMatch = 0;

    for ( tWatchedTree * watchedTree = g.treeList; watchedTree != NULL; watchedTree = watchedTree->next )
    {
        int matchLen = 0;
        const char * d = dir;
        const char * p = watchedTree->root.path;
        while ( *p == *d && *p != '\0' )
        { ++p; ++d; ++matchLen; }

        if ( (*d != '/') && (*d != '\0') )
        {
            // The match did not end at a root.path separator - i.e. it terminated
            // midway through a root.path component. Therefore, it is not a match.
            matchLen = 0;
        }

        if (matchLen > longestMatch)
        {
            longestMatch = matchLen;
            if ( longestMatch > 2 )
            { result = watchedTree; }
        }
    }
    return result;
}
#endif

tWatchedTree * createTree( const char * dir )
{
    int result = 0;

    tWatchedTree * watchedTree;

    fprintf( stderr,  " processing tree \'%s\'\n", dir );

    watchedTree = calloc( 1, sizeof(tWatchedTree) );
    if ( watchedTree != NULL )
    {
        watchedTree->root.path = normalizePath( dir );
        if ( watchedTree->root.path == NULL )
        {
            fprintf( stderr, "Error: unable to normalize the root.path \'%s\' (%d: %s)\n",
                     dir, errno, strerror(errno));
        }
        else
        {
            fprintf( stderr,  " absolute root.path \'%s\'\n", watchedTree->root.path );
            watchedTree->root.pathLen = strlen( watchedTree->root.path );
            watchedTree->root.fd = open( watchedTree->root.path, O_DIRECTORY );
            if ( watchedTree->root.fd < 0 )
            {
                fprintf( stderr, "Error: couldn't open the \'%s\' directory (%d: %s)\n",
                         dir, errno, strerror(errno));
                result = -errno;
            } else
            {
                result = makeSeenDir( watchedTree );
            }
        }
        fprintf( stderr, "root.fd: %d, seen: %d\n", watchedTree->root.fd, watchedTree->seen.fd );

        // structure was calloc'd, so unnecessary
        // watchedTree->watchHashes  = NULL;
        // watchedTree->pathHashes   = NULL;
        // watchedTree->cookieHashes = NULL;
        // watchedTree->expiringList = NULL;

        watchedTree->nextRescan = time(NULL) + 60;

        watchedTree->inotify.fd  = inotify_init();
        if ( watchedTree->inotify.fd == -1 )
        {
            fprintf( stderr, "Unable to register for filesystem events (%d: %s)\n",
                     errno, strerror(errno) );
            result = -errno;
        } else {
            struct epoll_event epollEvent;
            epollEvent.events   = EPOLLIN;
            epollEvent.data.ptr = watchedTree;

            if ( epoll_ctl( g.epollfd, EPOLL_CTL_ADD, watchedTree->inotify.fd, &epollEvent) == -1 )
            {
                fprintf( stderr, "unable to register inotify.fd fd %d with epoll fd %d (%d: %s)\n",
                         watchedTree->inotify.fd, g.epollfd, errno, strerror(errno) );
                result = -errno;
            }
        }
        if (result  != 0)
        {
            free( watchedTree );
            watchedTree = NULL;
        }
        else
        {
            watchedTree->next = g.treeList;
            g.treeList = watchedTree;
        }
    }

    return watchedTree;
}


/**
 * @brief
 */
tError startDaemon(void)
{
    tError result = 0;
    int pid = fork();
    switch (pid)
    {
    case -1: // failed
        result = -errno;
        break;

    case 0: // child
        {
            pid = setsid();
            size_t len = sizeof("/var/run/") + strlen(g.executableName) * 2 + sizeof('/') + sizeof(".pid");
            char * pidName = calloc( 1, len );
            snprintf( pidName, len, "/var/run/%s", g.executableName );
            if ( mkdir(pidName, S_IRWXU ) == -1 && errno != EEXIST )
            {
                fprintf( stderr, "Error: unable to create %s (%d: %s)\n",
                         pidName, errno, strerror(errno));
            }
            else
            {
                snprintf( pidName, len, "/var/run/%s/%s.pid", g.executableName, g.executableName );
                FILE * pidFile = fopen( pidName, "w" );
                if ( pidFile == NULL )
                {
                    fprintf( stderr, "Error: unable to open %s for writing (%d: %s)\n",
                             pidName, errno, strerror(errno) );
                }
                else
                {
                    if ( fprintf( pidFile, "%d", pid ) == -1 )
                    {
                        fprintf( stderr, "Error: unable to write pid to %s (%d: %s)\n",
                                 pidName, errno, strerror(errno) );
                    }
                }
            }
        }
        return scanLoop();

    default: // parent
        break;
    }
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

    /* the global arg_xxx structs above are initialised within the argtable */
    void * argtable[] =
    {
        g.option.help       = arg_litn( "h", "help", 0, 1,
                                    "display this help (and exit)" ),
        g.option.version    = arg_litn( "V", "version", 0, 1,
                                    "display version info (and exit)" ),
        g.option.zero       = arg_litn("0", "zero", 0, 1,
                                     "terminate the paths being output with a null" ),
        g.option.path       = arg_filen(NULL, NULL, "<file>", 1, 20,
                                    "input files" ),

        g.option.end        = arg_end( 20 )
    };

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
    else
    {
        g.epollfd = epoll_create1( 0 );

        if (result == 0)
        {
            tWatchedTree * watchedTree;
            for ( int i = 0; i < g.option.path->count && result == 0; i++ )
            {
                watchedTree = createTree( g.option.path->filename[ i ] );
                result = scanTree( watchedTree );
            }
        }
        if (result == 0)
        {
            result = startDaemon();
        }
    }

    /* release each non-null entry in argtable[] */
    arg_freetable( argtable, sizeof( argtable ) / sizeof( argtable[0] ));

    return result;
}
