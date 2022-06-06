//
// Created by paul on 6/5/22.
//

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <dlfcn.h>
#include <fcntl.h>
#include <stdbool.h>

#include <argtable3.h>
#include <unistd.h>
#include <dirent.h>

struct {
    const char *       executableName;
    FILE *             outputFile;
    int                debugLevel;
    struct
    {
        int root;
        int seen;
        int queue;
    } dir;
    struct
    {
        struct arg_lit  * help;
        struct arg_lit  * version;
        struct arg_file * path;
        struct arg_end  * end;
    } option;
} g;

const char * catstrings( const char * front, const char * back )
{
    char * result;
    int l = strlen( front ) + strlen(back ) + 1;
    result = calloc( l, sizeof(char) );
    if ( result != NULL )
    {
        strncpy( result, front, l );
        strncat( result, back, l );
    }
    return (const char *)result;
}

int makeSubDir( const char * subdir )
{
    int result = 0;

    if ( mkdirat( g.dir.root, subdir, S_IRWXU | S_IRWXG) < 0 && errno != EEXIST )
    {
        fprintf( stderr, "Error: cound't create subdirectory \'%s\' (%d: %s)\n",
                 subdir, errno, strerror(errno));
        result = -errno;
    } else {
        result = openat( g.dir.root, subdir, O_DIRECTORY );
        if ( result < 0 )
        {
            fprintf( stderr, "Error: cound't open subdirectory \'%s\' (%d: %s)\n",
                     subdir, errno, strerror(errno));
            result = -errno;
        }
    }
    return result;
}

int processDirectory( const char * dir )
{
    int result = 0;

    printf( " processing \'%s\'\n", dir );

    g.dir.root  = open( dir, O_DIRECTORY );
    if ( g.dir.root < 0 )
    {
        fprintf( stderr, "Error: cound't open \'%s\' (%d: %s)\n",
                 dir, errno, strerror(errno));
        result = -errno;
    } else {
        g.dir.seen = makeSubDir( ".seen" );
        if ( g.dir.seen < 0 )
        {
            result = g.dir.seen;
        }
        g.dir.queue = makeSubDir( ".queue" );
        if ( g.dir.queue < 0 )
        {
            result = g.dir.queue;
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
            g.option.help    = arg_litn( "h", "help", 0, 1,
                                        "display this help (and exit)" ),
            g.option.version = arg_litn( "V", "version", 0, 1,
                                        "display version info (and exit)" ),
            g.option.path    = arg_filen(NULL, NULL, "<file>", 1, 20,
                                        "input files" ),

            g.option.end     = arg_end( 20 )
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