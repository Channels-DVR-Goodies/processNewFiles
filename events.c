//
// Created by paul on 10/1/22.
//

#include "processNewFiles.h"

#include <poll.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <time.h>
#include <ftw.h>
#include <sys/inotify.h>

#include "events.h"
#include "logStuff.h"
#include "rescan.h"
#include "inotify.h"

typedef enum {
    kSignalEvent = 1,  /* signal received */
} tEpollSpecialValue;

static struct {
    struct {
        tFileDscr   fd;
    } epoll;

    struct {
        tFileDscr   fd;
    } signal;

    tWatchedTree *  treeList;
} gEvent;


#if 0
/**
 * @brief
 * @param fsNode
 */
void freeNode( tFSNode * fsNode )
{
    if (fsNode != NULL ) {
        if ( fsNode->path != NULL ) {
            free( (void *) fsNode->path );
        }
        free( fsNode );
    }
}
#endif

/* to be used in messages of the form 'because it %s' */
const char * const expiredReasonAsStr[] = {
        [kUnmonitored] = "is not monitored",
        [kFirstSeen]   = "is new",
        [kModified]    = "has been modified",
        [kMoved]       = "has moved",
        [kRetry]       = "is being retried"
};

/**
 *
 * @param signum
 */
void logSignal( unsigned int signum )
{
    const char * signalInfo[][2] = {
            [SIGHUP]    = { "SIGHUP",    "Hangup" },
            [SIGINT]    = { "SIGINT",    "Interactive attention signal" },
            [SIGQUIT]   = { "SIGQUIT",   "Quit" },
            [SIGILL]    = { "SIGILL",    "Illegal instruction" },
            [SIGTRAP]   = { "SIGTRAP",   "Trace/breakpoint trap" },
            [SIGABRT]   = { "SIGABRT",   "Abnormal termination" },
            [SIGFPE]    = { "SIGFPE",    "Erroneous arithmetic operation" },
            [SIGKILL]   = { "SIGKILL",   "Killed" },
            [SIGSEGV]   = { "SIGSEGV",   "Invalid access to storage" },
            [SIGPIPE]   = { "SIGPIPE",   "Broken pipe" },
            [SIGALRM]   = { "SIGALRM",   "Alarm clock" },
            [SIGTERM]   = { "SIGTERM",   "Termination request" },
            [SIGSTKFLT] = { "SIGSTKFLT", "Stack fault" },
            [SIGCHLD]   = { "SIGCHLD",   "Child exited" },
            [SIGCONT]   = { "SIGCONT",   "Continued" },
            [SIGSTOP]   = { "SIGSTOP",   "Stopped (signal)" },
            [SIGTSTP]   = { "SIGTSTP",   "Stopped" },
            [SIGTTIN]   = { "SIGTTIN",   "Stopped (tty input)" },
            [SIGTTOU]   = { "SIGTTOU",   "Stopped (tty output)" },
            [SIGURG]    = { "SIGURG",    "Urgent I/O condition" },
            [SIGXCPU]   = { "SIGXCPU",   "CPU time limit exceeded" },
            [SIGXFSZ]   = { "SIGXFSZ",   "File size limit exceeded" },
            [SIGVTALRM] = { "SIGVTALRM", "Virtual timer expired" },
            [SIGPROF]   = { "SIGPROF",   "Profiling timer expired" },
            [SIGWINCH]  = { "SIGWINCH",  "Window changed" },
            [SIGPOLL]   = { "SIGPOLL",   "I/O possible" },
            [SIGPWR]    = { "SIGPWR",    "Power failure" },
            [SIGSYS]    = { "SIGSYS",    "Bad system call" },
    };

    const char * sigName = "(unknown)";
    const char * sigDesc = "(unknown)";
    if ( signum <= SIGSYS) {
        if ( signalInfo[signum][0] != NULL ) {
            sigName = signalInfo[signum][0];
        }
        if ( signalInfo[signum][1] != NULL ) {
            sigDesc = signalInfo[signum][1];
        }
    }

    logInfo( "daemon received signal %d: %s (%s)", signum, sigDesc, sigName );
}


/**
 * @brief
 * @param siginfo
 * @return
 */
int processOneSignalEvent( struct signalfd_siginfo * siginfo )
{
    int result = 0;

    logSignal( siginfo->ssi_signo );
    switch ( siginfo->ssi_signo )
    {
    case SIGINT:
        result = -EINTR;
        break;

    case SIGCHLD:
        logInfo( "Child exit status %d", siginfo->ssi_status );
        break;

    default:
        break;
    }

    return result;
}


/**
 * @brief
 * @return
 */
int processSignalEvents( void )
{
    int result = 0;

    static char buf[ 8 * sizeof(struct signalfd_siginfo) ];

    ssize_t len = read( gEvent.signal.fd, buf, sizeof( buf ));

    /* If the nonblocking read() found nothing to read, then it returns
     * -1 with errno set to EAGAIN. That's a normal case, not an error */
    if ( len == -1 && errno != EAGAIN ) {
        result = -errno;
        logError( "unable to read from signal fd %d", gEvent.signal.fd );
    } else {
        /* Loop over all siginfo events packed into the buffer we just filled */
        int count = 0;
        const char * event = buf;
        const char * end   = buf + len;
        while ( event < end ) {
            ++count;
            result = processOneSignalEvent( (struct signalfd_siginfo *)event );
            event += sizeof( struct signalfd_siginfo );
        }
        logDebug( "processed %d signal events from a buffer %ld bytes long", count, len );
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

    // logDebug( "epoll bitmask 0x%x, data %p", epollEvent->events, epollEvent->data.ptr );
    /* Process an epoll event we just read from the epoll file descriptor. */
    if ( epollEvent->events & EPOLLIN )
    {
        /* check for the 'special values' */
        switch ( epollEvent->data.u64 )
        {
        case kSignalEvent:
            result = processSignalEvents();
            break;

        default:
            /* if it's not a 'special' event, then iNotify events are waiting,
             * and data.ptr points at the corresponding watchedTree */
            result = processInotifyEvents(  (tWatchedTree *)epollEvent->data.ptr );
            break;
        }
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
 * @brief execute the processing for the provided fileNode
 * if it fails, it will be added to the expiring list with a new
 * expiration time. Otherwise, it'll keep getting retried immediately,
 * and everything else will back up behind it
 * @param fileNode
 * @return
 */
tError readyToExec(tFSNode * fileNode )
{
    tError   result = 0;

    logDebug( "### ready to execute %s", fileNode->path );

    fileNode->next = g.readyList;
    g.readyList = fileNode;
    ++g.readyCount;

    return result;
}


/**
 * @brief
 * @param node
 * @return
 */
tError fileExpired( tFSNode * node )
{
    tError   result = 0;
    const tWatchedTree * watchedTree = node->watchedTree;

    logDebug("\'%s\' has node, and %s", node->relPath, expiredReasonAsStr[ node->expires.because ] );

    /* remove any existing shadow file */
    if (unlinkat(watchedTree->shadow.fd, node->relPath, 0 ) == -1 && errno != ENOENT ) {
        logError("unable to remove shadow file \'%s\'", node->relPath );
    }
    /* create a fresh shadow file */
    int fd = openat(watchedTree->shadow.fd, node->relPath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU );
    if ( fd == -1 ) {
        result = -errno;
        logError("unable to create shadow file \'%s/%s\'", watchedTree->shadow.path, node->relPath );
    } else {
        char * buffer;

        if ( asprintf(&buffer, "#!/bin/bash\nFILE=\'%s\'\nREASON=\'%s\'\n%s\n",
                      node->path,
                      expiredReasonAsStr[ node->expires.because ],
                      watchedTree->exec ) < 1 ) {
            logDebug( "unable to generate script contents" );
        }
        ssize_t len = strlen( buffer );
        if ( write( fd, buffer, len) < len ) {
            logError( "unable to write script contents" );
        }
        free( buffer );
        close( fd );

        result = readyToExec(node);
    }

    return result;
}


/**
 * @brief scan the expiringList of each watchedTree, expiring entries whose time has passed
 * @return
 */
tError processExpiredFSNodes( void )
{
    int result = 0;
    tFSNode * node;

#ifdef DEBUG
    int count = 0;
    for( node = g.expiringList; node != NULL; node = node->next )
    {
        ++count;
        logDebug("%s expires in %ld secs", node->path, node->expires.at - time(NULL) );
    }
     if (count > 0) {
        logDebug( "%d %s waiting to expire", count, count > 1 ? "nodes" : "node" );
    }
#endif

    time_t now = time( NULL );
    while ((node = g.expiringList) != NULL
           && node->expires.at <= now)
    {
        /* remove the first item on the expiringList because it expired... */
        g.expiringList = node->next;

        switch (node->type)
        {
        case kFile:
            result = fileExpired( node );
            break;

        case kTree:
            resetExpiration( node, kRescan );
            result = rescanTree( node->watchedTree );
            break;

        default:
            logError( "(Internal) something expired that isn't supposed to expire!" );
            break;
        }

        /* if there was a problem, exit the loop */
        if (result != 0) break;
    }

    return result;
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

    if ( g.expiringList != NULL) {
        LL_DELETE2(g.expiringList, fsNode, next );
    }

    tWatchedTree * watchedTree = fsNode->watchedTree;

    if (watchedTree != NULL) {
        forgetWatch(fsNode);
        if ( watchedTree->pathHashMap != NULL) {
            HASH_DELETE( pathHandle, watchedTree->pathHashMap, fsNode );
        }
    }
}


/**
 * @brief
 * @param fileNode
 */
void markFileComplete( tFSNode * fileNode )
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

    forgetNode( fileNode );
    /* ToDo: free the fileNode */
}


/**
 * @brief
 * @param fileNode
 * @return
 */
tError retryFileNode( tFSNode * fileNode )
{
    tError result = 1;

    fileNode->expires.retries++;    /* count the failures, so we don't retry forever */
    fileNode->expires.wait += 2;    /* increase the idle delay each time it fails */
    /* spread the expirations out over time, to spread the load if many of them
     * fail quickly and would otherwise be retried at almost the same time. This
     * is most likely to happen if the 'exec' statement provided by the user is
     * faulty in some way, e.g. doesn't have sufficient permissions */
    // fileNode->idlePeriod += (random() % g.timeout.idle);

    if ( fileNode->expires.retries >= 5 ) {
        logError( "failed to process \'%s\' successfully after %d retries",
                  fileNode->path,
                  fileNode->expires.retries );
        result = -ENOTRECOVERABLE;
    } else {
        logError( "attempt %d to process \'%s\' failed, retry",
                  fileNode->expires.retries,
                  fileNode->relPath );
        /* put it back onto the expireList to try again */
        resetExpiration( fileNode, kRetry );
    }
    return result;
}


/**
 *
 * @param fileNode
 * @return
 */
tError executeNode( tFSNode * fileNode )
{
    tError result = 0;

    /* ToDo: Actually execute it */
    result = ( fileNode->expires.retries < 2 )? -1 : 0 ;

    if (result == 0) {
        /* processing completed without error, so adjust the shadow file accordingly */
        markFileComplete( fileNode );
    } else {
        result = retryFileNode( fileNode );
    }

    return result;
}


/**
 * @brief figure out when the next soonest expiration will occur
 * @return time_t of the next expiration
 */
time_t nextExpiration( void )
{
    time_t whenExpires = (unsigned)(-1L); // the end of time...
#ifdef DEBUG
    const char * whatExpires  = NULL;
    tExpiredReason whyExpires = kUnmonitored;
#endif

    /* list is sorted ascending by expiration, so the head of the list is the next to expire */
    tFSNode const * node = g.expiringList;
    if (node != NULL && node->expires.at != 0 && whenExpires > node->expires.at ) {
        whenExpires = node->expires.at;
#ifdef DEBUG
        whyExpires  = node->expires.because;
        whatExpires = node->path;
#endif
    }
    time_t now = time( NULL );
    if ( whenExpires < now ) {
        whenExpires = now + 1;
    } else if ( whenExpires - now > g.timeout.rescan ) {
        whenExpires = now + g.timeout.rescan;
    }

    logDebug( "%s %s, and will expire in %ld seconds",
              whatExpires, expiredReasonAsStr[ whyExpires ], whenExpires - now );

    return whenExpires;
}


/**
 * @brief main event loop
 * @return
 */
tError eventLoop( void )
{
    int                result = 0;
    struct epoll_event epollEvents[32];

    rescanAllTrees();

    do {
        int timeout = (int)(nextExpiration() - time( NULL ));

        /* * * * * * block waiting for events or a timeout * * * * * */
        logSetErrno( 0 );
#if 0
        logDebug( "epoll_wait( fd: %d, %p[%ld], %d secs )",
                  gEvent.epoll.fd,
                  epollEvents,
                  sizeof( epollEvents ) / sizeof( struct epoll_event ),
                  timeout );
#endif
        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
        int count = epoll_wait( gEvent.epoll.fd,
                                epollEvents,
                                sizeof( epollEvents ) / sizeof( struct epoll_event ),
                                timeout * 1000 );
        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
        if ( count < 0 ) {
            /* something unexpected happened */
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
            /* epoll() delivered inotify events, signal events and/or an expiration */
            logDebug( "epoll reported %d %s", count, count == 1 ? "event" : "events" );

            /* If any nodes expired and became ready, put them on the ready list,
             * waiting for a SIGCHLD event to arrive */
            if ( result == 0 ) {
                result = processExpiredFSNodes();
            }

            /* Process any inotify and/or signal events that epoll returned */
            for ( int i = 0; i < count && result == 0; ++i ) {
                result = processEpollEvent( &epollEvents[ i ] );
            }
        }
    } while ( result == 0 );

    logInfo( "loop terminated %d", result );
    removePIDfile();
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

    if ( asprintf( (char **)&(watchedTree->shadow.path), "%s/.seen", watchedTree->root.path ) < 1 ) {
        logError( "failed to generate path to shadow directory" );
    }
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
    if ( rootPath == NULL ) {
        logError( "unable to normalize the root path \'%s\'", dir );
        result = -errno;
    } else {
        struct stat fileInfo;
        if ( stat( rootPath, &fileInfo ) == -1 ) {
            result = -errno;
            logError( "couldn't get info about %s", watchedTree->root.path );
        } else if ( S_ISDIR( fileInfo.st_mode ) ) {
            watchedTree->root.path    = rootPath;
            watchedTree->root.pathLen = strlen( rootPath );

            logDebug( "absolute root.path is \'%s\'", watchedTree->root.path );

            watchedTree->root.fd = open( watchedTree->root.path, O_DIRECTORY );
            if ( watchedTree->root.fd < 0 ) {
                logError( "couldn't open the root directory \'%s\'", watchedTree->root.path );
                result = -errno;
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
 * @param dir
 * @return
 */
tError createTree( const char * dir, const char * exec )
{
    int result = 0;

    if ( dir == NULL ) return -EINVAL;

    tWatchedTree * watchedTree = (tWatchedTree *)calloc( 1, sizeof( tWatchedTree ));
    if ( watchedTree == NULL ) {
        result = -ENOMEM;
    } else {
        logDebug( "creating tree for \'%s\'", dir );

        /* since calloc() was used for this structure, the pointers it contains are already NULL */
        tFSNode * node = calloc( 1,sizeof(tFSNode) );
        if ( node == NULL ) {
            free( watchedTree );
            return -ENOMEM;
        }


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
            node->type         = kTree;
            node->watchedTree  = watchedTree;
            node->path         = watchedTree->root.path;
            node->relPath      = &node->path[ strlen(node->path) ];

            watchedTree->next  = gEvent.treeList;
            gEvent.treeList    = watchedTree;

            node->expires.at   = time( NULL ) + 1;
            node->expires.wait = g.timeout.rescan;

            resetExpiration( node, kRescan );
        } else {
            free( (void *)watchedTree->exec );
            free( (void *)watchedTree->root.path );
            free( (void *)watchedTree->shadow.path );
            free( watchedTree );
            free( node );
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
        if ( asprintf( &pidDir, "/tmp/%s", g.executableName ) < 1 ) {
            logError( "unable to create the PID filename" );
        }
        if ( mkdir( pidDir, S_IRWXU) == -1 && errno != EEXIST ) {
            result = -errno;
            logError( "Error: unable to create directory %s", pidDir );
        } else {
            if ( asprintf( &g.pidFilename,"/tmp/%s/%s.pid",
                           g.executableName, g.executableName ) < 1 ) {
                logError( "unable to generate path to pid file" );
            }
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
        result = -errno;
        logError( "Error: unable to open %s for reading",
                  g.pidFilename );
    } else {
        char pidStr[ 32 ];
        char * pidPtr = fgets( pidStr, sizeof(pidStr), pidFile );
        if ( pidPtr == NULL ) {
            result = -errno;
            logError( "unable to read pid from %s", g.pidFilename );
        } else {
            result = (pid_t)strtoul( pidStr, &pidPtr, 10 );
            if ( *pidPtr != '\0' ) {
                result = -EINVAL;
                logError( "unable convert %s to an integer", pidStr );
            }
        }
        fclose( pidFile );
    }

    return result;
}


#if 0
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
void daemonSignal( unsigned int signum )
{
    logSignal( signum );

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
#endif

tError registerFdToEpoll( tFileDscr fd, uint64_t data )
{
    tError             result = 0;
    struct epoll_event epollEvent;

    epollEvent.events = EPOLLIN;
    epollEvent.data.u64 = data;

    if ( epoll_ctl( gEvent.epoll.fd,
                    EPOLL_CTL_ADD,
                    fd,
                    &epollEvent ) == -1 )
    {
        result = -errno;
        logError( "unable to register fd %d with epoll fd %d",
                  fd, gEvent.epoll.fd );
    }
    return result;
}


/**
 * @brief prepare the main event loop
 * @return non-zero if the initialization failed
 */
tError initEventLoop( void )
{
    tError result = 0;
    gEvent.epoll.fd = epoll_create1( 0 );


    sigset_t mask;
    sigfillset( &mask );
    sigdelset( &mask, SIGSTOP ); /* can't block or use with signalfd */
    sigdelset( &mask, SIGKILL ); /* can't block or use with signalfd */

    gEvent.signal.fd = signalfd( -1, &mask, SFD_CLOEXEC );
    if ( gEvent.signal.fd == -1 ) {
        result = errno;
        logError( "unable to allocate a new signal fd" );
    } else {

        sigprocmask( SIG_BLOCK, &mask, NULL );

        result = registerFdToEpoll( gEvent.signal.fd, kSignalEvent );

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
        result = initEventLoop();
    }

#if 0
    if ( result == 0 ) {
        result = registerHandlers();
    }
#endif

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


/**
 * @brief
 */
void stopDaemon( void )
{
    logCheckpoint();
}
