//
// Created by paul on 6/5/22.
//

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

#include <dlfcn.h>
#include <fcntl.h>

#include <argtable3.h>
#include <unistd.h>
#include <ftw.h>

typedef struct {
    const char * path;
    int          fd;
} tDirectory;

struct {
    const char *       executableName;
    FILE *             outputFile;
    int                debugLevel;

    tDirectory  root;
    tDirectory  seen;
    tDirectory  queue;

    struct
    {
        struct arg_lit  * help;
        struct arg_lit  * version;
        struct arg_file * executable;
        struct arg_file * path;
        struct arg_end  * end;  // must be last !
    } option;
} g;

const char * catPath( const char * front, const char * back )
{
    char * result;
    size_t l = strlen( front ) + 1 + strlen(back ) + 1;
    result = calloc( l, sizeof(char) );
    if ( result != NULL )
    {
        strncpy( result, front, l );
        strncat( result, "/", l );
        strncat( result, back, l );
    }
    return (const char *)result;
}

int makeSubDir( const char * subDirPath, tDirectory * dir )
{
    int result = 0;

    dir->path    = catPath( g.root.path, subDirPath );
    if ( mkdirat( g.root.fd, subDirPath, S_IRWXU | S_IRWXG) < 0 && errno != EEXIST )
    {
        fprintf( stderr, "Error: couldn't create subdirectory \'%s\' (%d: %s)\n",
                 subDirPath, errno, strerror(errno));
        result = -errno;
    } else {
        dir->fd = openat( g.root.fd, subDirPath, O_DIRECTORY );
        if ( dir->fd < 0 )
        {
            fprintf( stderr, "Error: couldn't open subdirectory \'%s\' (%d: %s)\n",
                     subDirPath, errno, strerror(errno));
            result = -errno;
        }
    }
    return result;
}

int deleteOrphan( const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
    int result = FTW_CONTINUE;
    const char * base = &fpath[ ftwbuf->base ];

    switch ( typeflag )
    {
    case FTW_F: // fpath is a regular file.
        if ( *base != '.' )
        {
            printf( "%d: file: %s (%lu)\n", ftwbuf->level, base, sb->st_nlink );
            if ( sb->st_nlink == 1 )
            {
                printf( "delete orphan: %s\n", fpath );
                unlink( fpath );
            }
        }
        break;

    case FTW_D: // fpath is a directory.
        if ( *base == '.' && ftwbuf->level > 0)
        {
            result = FTW_SKIP_SUBTREE;
        } else
        {
            printf( "%d: directory: %s\n", ftwbuf->level, base );
        }
        break;

    default:fprintf( stderr, "Error: %d: typeflag %d\n", ftwbuf->level, typeflag );
        break;
    }
    return result;
}


int linkOrphan( const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
    int result = FTW_CONTINUE;
    const char * base = &fpath[ ftwbuf->base ];
    const char * relpath = NULL;

    const char * r = g.root.path;
    const char * p = fpath;

    do { ++r; ++p; } while ( *r != '\0' && *r == *p );
    if (*r == '\0')
    {
        if (*p == '/') { ++p; }
        relpath = p;
        printf("relPath: \'%s\'\n", relpath );
    }

    switch ( typeflag )
    {
    case FTW_F: // fpath is a regular file.
        if ( *base != '.')
        {
            printf("%d: file: %s (%lu)\n", ftwbuf->level, base, sb->st_nlink );
            if ( sb->st_nlink == 1 )
            {
                printf("new file: %s\n", relpath );
                linkat( g.root.fd, relpath, g.seen.fd,  relpath, 0 );
                linkat( g.root.fd, relpath, g.queue.fd, relpath, 0 );
            }
        }
        break;

    case FTW_D: // fpath is a directory.
        if ( *base == '.')
        {
            result = FTW_SKIP_SUBTREE;
        } else {
            printf("%d: directory: %s\n", ftwbuf->level, base );
            mkdirat(g.seen.fd, relpath, S_IRWXU | S_IRWXG );
            mkdirat(g.queue.fd, relpath, S_IRWXU | S_IRWXG );
        }
        break;

    default:
        fprintf( stderr, "Error: %d: typeflag %d\n", ftwbuf->level, typeflag );
        break;
    }
    return result;
}

int processFile( const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf )
{
    int result = FTW_CONTINUE;
    const char * base = &fpath[ ftwbuf->base ];

    if (ftwbuf->level > 0)
    {
        switch ( typeflag )
        {
        case FTW_F: // fpath is a regular file.
            if ( *base != '.')
            {
                printf("%d: execute with file: %s (%lu)\n", ftwbuf->level, fpath, sb->st_nlink );
            }
            break;

        case FTW_D: // fpath is a directory.
            if ( *base == '.' && ftwbuf->level > 0)
            {
                result = FTW_SKIP_SUBTREE;
            } else {
                printf("%d: directory: %s\n", ftwbuf->level, base );
            }
            break;

        default:
            fprintf( stderr, "Error: %d: typeflag %d\n", ftwbuf->level, typeflag );
            break;
        }
    }
    return result;
}


int processDirectory( const char * dir )
{
    int result = 0;

    printf( " processing \'%s\'\n", dir );

    g.root.path    = strdup( dir );
    g.root.fd      = open( dir, O_DIRECTORY );
    if ( g.root.fd < 0 )
    {
        fprintf( stderr, "Error: couldn't open \'%s\' (%d: %s)\n",
                 dir, errno, strerror(errno));
        result = -errno;
    } else {
        result = makeSubDir( ".seen", &g.seen );
        if ( result == 0 )
        {
            result = makeSubDir( ".queue", &g.queue );
        }
    }

    if ( result == 0 )
    {
        // scan for orphans, and delete them
        result = nftw( g.seen.path, deleteOrphan, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
        if (result != 0)
        {
            fprintf( stderr, "Error: couldn't prune orphans in the \'%s\'  directory (%d: %s)",
                     g.seen.path, errno, strerror(errno) );
        } else {
            // scan for files we have not seen yet, and hard-link them to 'seen' and 'queue'
            result = nftw( g.root.path, linkOrphan, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
            if (result != 0)
            {
                fprintf( stderr, "Error: couldn't link new files in the \'%s\' directory (%d: %s)",
                         g.root.path, errno, strerror(errno) );
            } else {
                // scan the queue for files we have not successfully processed yet
                result = nftw( g.queue.path, processFile, 12, FTW_ACTIONRETVAL | FTW_MOUNT );
                if (result != 0)
                {
                    fprintf( stderr, "Error: couldn't process queued files in the \'%s\' directory (%d: %s)",
                             g.queue.path, errno, strerror(errno));
                }
            }
        }
    }
    return result;
}

/**
 * @brief validate the path before calling processDirectory()
 * @param path
 * @return 0 on success, -errno on failure
 */
int processPath(const char * path)
{
    int result = 0;
    const char * absPath;
    struct stat info;
    absPath = realpath( path, NULL );
    if (absPath == NULL )
    {
        fprintf( stderr, "Error: cound't convert \'%s\' into an absolute path (%d: %s)\n",
                 path, errno, strerror(errno));
        result = -errno;
    } else {
        if ( stat( absPath, &info ) < 0)
        {
            fprintf( stderr, "Error: cound't get info about \'%s\' (%d: %s)\n",
                     absPath, errno, strerror(errno));
            result = -errno;
        } else {
            if ( ! S_ISDIR(info.st_mode) )
            {
                errno = ENOTDIR;
                fprintf( stderr, "Error: \'%s\' is not a directory (%d: %s)\n",
                         absPath, errno, strerror(errno) );
                result = -errno;
            } else {
                if ( access( absPath, W_OK ) == -1 )
                {
                    fprintf( stderr, "Error: cannot write to directory \'%s\' (%d: %s)\n",
                             absPath, errno, strerror(errno) );
                    result = -errno;
                }
            }
        }

        if ( result == 0)
        {
            result = processDirectory(absPath);
        }
        free( (void *)absPath );
    }
    return result;
}

int main( int argc, char * argv[] )
{
    int result = 0;

    g.executableName = strrchr( argv[0], '/' );
    /* If we found a slash, increment past it. If there's no slash, point at the full argv[0] */
    if ( g.executableName++ == NULL)
    {
        g.executableName = argv[0];
    }

    g.outputFile = stdout;

    /* the global arg_xxx structs above are initialised within the argtable */
    void * argtable[] =
        {
            g.option.help       = arg_litn( "h", "help", 0, 1,
                                        "display this help (and exit)" ),
            g.option.version    = arg_litn( "V", "version", 0, 1,
                                        "display version info (and exit)" ),
            g.option.executable = arg_filen("x", "exec", "<executable>", 1, 1,
                                         "path to executable" ),
            g.option.path       = arg_filen(NULL, NULL, "<file>", 1, 20,
                                        "input files" ),

            g.option.end        = arg_end( 20 )
        };

    int nerrors = arg_parse( argc, argv, argtable );

    if ( g.option.help->count > 0 )    /* special case: '--help' takes precedence over everything else */
    {
        fprintf( stdout, "Usage: %s", g.executableName );
        arg_print_syntax( stdout, argtable, "\n" );
        fprintf( stdout, "process hash file into a header file.\n\n" );
        arg_print_glossary( stdout, argtable, "  %-25s %s\n" );
        fprintf( stdout, "\n" );

        result = 0;
    }
    else if ( g.option.version->count > 0 )   /* ditto for '--version' */
    {
        fprintf( stdout, "%s, version %s\n", g.executableName, "(to do)" );
    }
    else if ( nerrors > 0 )    /* If the parser returned any errors then display them and exit */
    {
        /* Display the error details contained in the arg_end struct.*/
        arg_print_errors( stdout, g.option.end, g.executableName );
        fprintf( stdout, "Try '%s --help' for more information.\n", g.executableName );
        result = 1;
    }
    else
    {
        g.outputFile = NULL;

        result = 0;
        for ( int i = 0; i < g.option.path->count && result == 0; i++ )
        {
            result = processPath( g.option.path->filename[i] );
        }
    }

    /* release each non-null entry in argtable[] */
    arg_freetable( argtable, sizeof( argtable ) / sizeof( argtable[0] ));

    return result;
}