//
// Created by paul on 10/19/22.
//

#include "processNewFiles.h"

#include <time.h>
#include <ftw.h>

#include "rescan.h"
#include "events.h"

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
                          const char *        fullPath,
                          const struct FTW *  ftwbuf )
{
    const char * status = "???";

    const char * relPath = &fullPath[ watchedTree->root.pathLen ];
    if ( fullPath[ ftwbuf->base ] != '.' )
    {
        struct stat fileInfo;
        if ( fstatat( watchedTree->shadow.fd, relPath, &fileInfo, 0  ) == -1 ) {
            switch (errno)
            {
            case ENOENT: /* shadow file does not exist, so create a fresh file node */
                watchNode( watchedTree, fullPath, kFile );
                status = "first seen";
                break;

            default: /* everything else is unexpected */
                logError( "Failed to get info about shadow file \'%s\'", relPath );
                break;
            }
        } else {
            if ( S_ISREG( fileInfo.st_mode ) )
            {
                if ( fileInfo.st_mode & (S_IXUSR | S_IXGRP) ) {
                    /* shadow file is *already* present and executable */
                    status = "retry";
                } else {
                    status = "seen";
                }
            } else {
                logError( "the shadow file \'%s/%s\' is not a regular file", watchedTree->shadow.path, relPath );
            }
        }
        (void)status;
//      logDebug( "%d%-*c file: %s (%s)", ftwbuf->level, ftwbuf->level * 4, ':', relPath, status );
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

    if ( fullPath[ ftwbuf->base ] != '.'
         && strcmp( fullPath, watchedTree->shadow.path ) != 0 )
    {
        result = FTW_CONTINUE;

        const char * relPath = &fullPath[ watchedTree->root.pathLen ];
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
            errno = 0;
        }

        watchNode( watchedTree, fullPath, kDirectory );
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

    if ( gWatchedTree != NULL ) {
        switch ( typeflag ) {
        case FTW_F: // fullPath is a regular file.
            result = scanFileNode( gWatchedTree, fullPath, ftwbuf );
            break;

        case FTW_D: // fullPath is a directory.
            result = scanDirNode( gWatchedTree, fullPath, ftwbuf );
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
 * @brief Walk the hierarchy for files we have not seen before.
 * This is a backstop for the iNotify event mechanism.
 * @param watchedTree
 * @return
 */
tError rescanTree( tWatchedTree * watchedTree )
{
    int result = 0;

    if ( watchedTree != NULL ) {

#ifdef DEBUG
    for( tFSNode * node = watchedTree->expiringList; node != NULL; node = node->expiringNext )
    {
        logDebug("%s %ld", node->relPath, node->expires - time( NULL ) );
    }
#endif

        gWatchedTree = watchedTree;

        result = nftw( watchedTree->root.path, scanNode, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
        if ( result != 0 ) {
            logError( "Error: failed to scan for new files in the \'%s\' directory", watchedTree->root.path );
            result = -errno;
        }

        gWatchedTree = NULL;

        watchedTree->nextRescan = time(NULL ) + g.timeout.rescan;
    }

    return result;
}

