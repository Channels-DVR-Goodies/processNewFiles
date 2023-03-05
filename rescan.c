//
// Created by paul on 10/19/22.
//

#include "processNewFiles.h"

#include <time.h>
#include <ftw.h>

#include "rescan.h"
#include "events.h"
#include "inotify.h"

#include "logStuff.h"

/* Unfortunately, the API for nftw() doesn't support the caller
 * providing an opaque structure to be passed through to the
 * function pointer. So we're forced to use a global */

tWatchedTree *  gWatchedTree;


/**
 * @brief
 * @param fullPath
 * @param ftwbuf
 * @param watchedTree
 * @return
 */
tNFTWresult scanFileNode( tWatchedTree *      watchedTree,
                          const struct stat * sb,
                          const char *        fullPath,
                          const struct FTW *  ftwbuf )
{
    const char * status = NULL;

    const char * relPath = &fullPath[ watchedTree->root.pathLen ];
    if ( *relPath == '/' ) {
        ++relPath;
    }

    if ( fullPath[ ftwbuf->base ] != '.' )
    {
        struct stat shadowInfo;
        if (fstatat(watchedTree->shadow.fd, relPath, &shadowInfo, 0  ) == -1 ) {
            if ( errno == ENOENT ) {
                /* shadow file does not exist, so create a fresh file node */
                fsNodeFromPath( watchedTree, fullPath, kFile );
                status = "first seen";
            } else {
                logError( "Failed to get info about shadow file \'%s\'", relPath );
            }
        } else if ( S_ISREG(shadowInfo.st_mode ) ) {
            if (shadowInfo.st_mode & (S_IXUSR | S_IXGRP) ) {
                /* shadow file is *already* present and executable */
                fsNodeFromPath( watchedTree, fullPath, kFile );
                status = "retry";
            }
            /* is the shadow file much older than the original? */
            if ( (sb->st_mtim.tv_sec - shadowInfo.st_mtim.tv_sec ) > g.timeout.idle ) {
                /* queue up the file to expire. Don't expire immediately in case we
                 * started up while the file was in the midst if being modified */
                fsNodeFromPath( watchedTree, fullPath, kFile );
                status = "modified";
            }
        } else {
            logError( "the shadow file \'%s/%s\' is not a regular file", watchedTree->shadow.path, relPath );
        }
        if ( status != NULL ) {
            logDebug( "%d%-*c file: %s (%s)", ftwbuf->level, ftwbuf->level * 4, ':', relPath, status );
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
                         const struct stat * sb,
                         const char *        fullPath,
                         const struct FTW *  ftwbuf )
{
    int result = FTW_SKIP_SUBTREE;
    (void)sb;

    if ( strncmp( fullPath,
                  watchedTree->shadow.path,
                  watchedTree->shadow.pathLen ) != 0
      && fullPath[ ftwbuf->base ] != '.' )
    {
        result = FTW_CONTINUE;

        const char * relPath = &fullPath[ watchedTree->root.pathLen ];
        if ( *relPath == '/' ) {
            ++relPath;
        }

#if 0
        logDebug( "%d%-*c  dir: %s",
                  ftwbuf->level,
                  ftwbuf->level * 4, ':',
                  relPath );
#endif

        /* make sure that the corresponding shadow directory exists */
        if ( strlen( relPath ) > 0 ) {
            if ( mkdirat( watchedTree->shadow.fd, relPath, S_IRWXU ) == -1
                && errno != EEXIST )
            {
                logError( "Unable to create directory %s{%s}",
                          watchedTree->shadow.path,
                          relPath );
            }
            logSetErrno( 0 );
        }

        fsNodeFromPath( watchedTree, fullPath, kDirectory );
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
    tNFTWresult result = FTW_CONTINUE;

    if ( gWatchedTree != NULL ) {
        switch ( typeflag ) {
        case FTW_F: // fullPath is a regular file.
            result = scanFileNode( gWatchedTree, sb, fullPath, ftwbuf );
            break;

        case FTW_D: // fullPath is a directory.
            result = scanDirNode( gWatchedTree, sb, fullPath, ftwbuf );
            break;

        default:
            logError( "Error: %d: unhandled typeflag %d for %s", ftwbuf->level, typeflag, fullPath );
            break;
        }
    }

    logSetErrno( 0 );
    return result;
}


/**
 * @brief Walk the hierarchy for files we have not seen before.
 * This is a backstop for the iNotify event mechanism.
 * @param watchedTree
 * @return
 */
tError rescanTree( tWatchedTree * watchedTree )
{
    int result = 0;

    if ( watchedTree != NULL ) {

        gWatchedTree = watchedTree;

        result = nftw( watchedTree->root.path, scanNode, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
        if ( result != 0 ) {
            logError( "Error: failed to scan for new files in the \'%s\' directory", watchedTree->root.path );
            result = -errno;
        }

        gWatchedTree = NULL;
    }

    return result;
}

tError rescanAllTrees( void )
{
    tError result = 0;
    tFSNode ** prev = &g.expiringList;
    for ( tFSNode * node = *prev; node != NULL; node = node->next )
    {
        if ( node->type == kTree )
        {
            /* unlink it from expiringList */
            *prev = node->next;
            /* force it to expire immediately */
            node->expires.at = time( NULL );
            /* put it first on expiringList */
            node->next = g.expiringList;
            g.expiringList = node;
        }
        prev = &node->next;
    }

    return result;
}