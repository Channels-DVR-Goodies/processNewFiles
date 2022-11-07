//
// Created by paul on 6/5/22.
//

#include "processNewFiles.h"

#include <sys/epoll.h>
#include <argtable3.h>

#include <libconfig.h>
#include <stdarg.h>

#include "events.h"
#include "logStuff.h"


/** gEvent */
tGlobals g;


/**
 * @brief
 * detach from parent, and become an independent process group leader
 * @return normally does not return, if it does, it's an error.
 */

tError printUsage( void ** argtable )
{
    fprintf( stdout, "Usage: %s", g.executableName );
    arg_print_syntax( stdout, argtable, "\n" );
    fprintf( stdout, "process watchHashMap file into a header file.\n\n" );
    arg_print_glossary( stdout, argtable, "  %-25s %s\n" );
    fprintf( stdout, "\n" );

    return 0;
}



#if 0
ssize_t appendString( char ** string, const char * fmt, ... )
__attribute__ ((format (printf, 2, 3)));
/**
 * @brief
 * @param string  the string to append to
 * @param fmt     "printf"-style format string
 * @param ...     the values for the fmt string
 * @return new length of string
 */
ssize_t appendString( char ** string, const char * fmt, ... )
{
    va_list  args;
    ssize_t  addLength;
    char *   addString;

    ssize_t len = strlen( *string );

    va_start( args, fmt );

    addLength = vasprintf( &addString, fmt, args );

    va_end( args );
    if ( addLength != -1 ) {
        *string = realloc( *string, len + addLength + 1 );
        strncpy( &(*string)[ len ], addString, addLength );
        free( addString );
    }

    return len + addLength;
}
#endif

tError importTree( const config_setting_t * group )
{
    tError result = 0;
    if ( config_setting_is_group( group ) ) {
        const char * path = NULL;
        const char * exec = NULL;
        const config_setting_t * member;

        member = config_setting_get_member( group, "path" );
        if ( member == NULL ) {
            logError( "in %s at line %d: watch group doesn't have a \'path\' element",
                      config_setting_source_file( group ),
                      config_setting_source_line( group ) );
            result = -EINVAL;
        } else {
            path = config_setting_get_string( member );
            if ( path != NULL ) {
                logDebug( "path = \"%s\"", path );
                member = config_setting_get_member( group, "exec" );
                if ( member != NULL ) {
                    exec = config_setting_get_string( member );
                    if ( exec != NULL ) {
                        logDebug( "exec = \"%s\"", exec );
                    }
                }
                result = createTree( path, exec );
            }
        }
    } else {
        logError( "in %s at line %d: watch entry must be a group",
                  config_setting_source_file( group ),
                  config_setting_source_line( group ) );
    }
    return result;
}

/**
 * @brief
 * @param config
 * @return
 */
tError importConfig( const config_t * config )
{
    tError result = 0;
#ifdef DEBUG
    fprintf( stdout, "###\n" );
    config_write( config, stdout );
#endif

    const config_setting_t * setting = config_lookup( config, "watch" );
    if ( setting == NULL ) {
        logError( "unable to find 'watch' element" );
        result = -EINVAL;
    } else {
        int count;
        switch ( setting->type )
        {
        case CONFIG_TYPE_GROUP:
            result = importTree( setting );
            break;

        case CONFIG_TYPE_LIST:
            count = config_setting_length( setting );
            logDebug("type is <list>, of length %d", count );
            for ( int i = 0; i < count; ++i ) {
                result = importTree( config_setting_get_elem( setting, i ) );
                if ( result != 0 ) break;
            }
            break;

        default:
            logError( "'watch' must be either a array or list of arrays" );
            result = -EINVAL;
            break;
        }
    }

    return result;
}

/**
 * @brief
 * @param config
 * @param path
 * @return
 */
int processConfigFile( config_t * config, const char * path )
{
    int result = 0;

    // config_clear( config );
    int err = config_read_file( config, path );
    logDebug( "config_read_file \'%s\' returned %d", path, err );

    if ( err == CONFIG_TRUE ) {
        result = importConfig( config );
    } else if ( err == CONFIG_FALSE ) {
        switch ( config_error_type( config ) )
        {
        case CONFIG_ERR_NONE:
            fprintf(stderr,
                    "config file \'%s\' not found\n",
                    path);
            break;

        case CONFIG_ERR_FILE_IO:
            fprintf(stderr, "config file I/O error\n");
            result = -1;
            break;

        case CONFIG_ERR_PARSE:
            fprintf( stderr,
                     "Configuration Error: %s: %s at line %d\n",
                     config_error_text( config ),
                     config_error_file( config ),
                     config_error_line( config ) );
            result = -1;
            break;

        default:
            fprintf( stderr, "unhandled case %s at line %d", __func__, __LINE__);
            break;
        }
    } else {
        logError( "config_read_file unexpectedly returned %d", err );
    }

    return result;
}

tError processConfigFiles( config_t * config, const struct arg_file * configFileArg )
{
    tError result;

    config_init( config );

    config_set_options( config, CONFIG_OPTION_ALLOW_OVERRIDES );

    char * configFile;

    asprintf( &configFile, "/etc/%s.conf", g.executableName );
    result = processConfigFile( config, configFile );
    free( configFile );

    if ( result == 0 ) {
        const char * home = getenv( "HOME" );
        if ( home != NULL ) {
            asprintf( &configFile,
                      "%s/.config/%s.conf",
                      home, g.executableName );
            result = processConfigFile( config, configFile );
            free( configFile );
        }
    }

    if ( result == 0 ) {
        for ( int i = 0; i < configFileArg->count; ++i ) {
            logDebug( "config file %d: %s", i, configFileArg->filename[ i ] );
            result = processConfigFile( config,
                                        configFileArg->filename[ i ] );
            if ( result != 0 ) break;
        }
    }
    return result;
}


/**
 * @brief tell a daemon process running in the background to exit
 *
 * Note: this will be called from another instance invoked with
 * the --kill option. That's why it's here, and not in events.c
 *
 * @return
 */
tError terminateDaemon( pid_t pid )
{
    tError result = 0;
    logDebug( "pid: \'%d\'", pid );
    if ( killpg( pid, SIGTERM ) == -1 ) {
        logError( "Error: failed to terminate the daemon" );
        result = -errno;
    }
    return result;
}


/**
 * @brief
 * @param argc
 * @param argv
 * @return
 */
tError processArgs( int argc, char * argv[] )
{
    tError       result;
    config_t *   config;

    static struct {
        struct arg_lit *   help;
        struct arg_lit *   version;
        struct arg_lit *   killDaemon;
        struct arg_int *   debugLevel;
        struct arg_file *  configFile;
        struct arg_file *  path;
        struct arg_end *   end;  // end of list: must be last!
    } option;

    /* the global arg_xxx structs above are initialised within the argtable */
    void * argtable[] =
    {
         option.help       =   arg_lit0( "h", "help",
                                               "display this help (and exit)" ),
         option.version    =   arg_lit0( "V", "version",
                                               "display version info (and exit)" ),
         option.killDaemon =   arg_lit0( "k", "kill",
                                               "shut down the background daemon (and exit)" ),
         option.debugLevel =   arg_int0( "d", "debug-level", "",
                                               "set the level of detail being logged (0-7, 0 is least detailed)" ),
         option.configFile =  arg_filen( "c", "config-file", "<file>",
                                               0, 10,
                                               "get configuration from the file provided" ),
         option.path       =  arg_filen( NULL, NULL, "<file>",
                                               0, 5,
                                               "input files" ),

         option.end        =    arg_end( 20 )
    };


    /* parse the command line arguments */
    result = arg_parse( argc, argv, argtable );

    if ( result > 0 ) {   /* If the parser returned any errors then display them and exit */
        /* Display the error details contained in the arg_end struct.*/
        arg_print_errors( stdout, option.end, g.executableName );
        fprintf( stdout, "Try '%s --help' for usage information.\n", g.executableName );
        return -EINVAL;
    } else {
        if ( option.help->count > 0 ) {  /* special case: '--help' takes precedence over everything else */
            return printUsage( argtable );
        } else if ( option.killDaemon->count > 0 ) {  /* ditto for '--kill' */
            return terminateDaemon( getDaemonPID());
        } else if ( option.version->count > 0 ) {     /* and for '--version' */
            fprintf(  stdout, "%s, version %s\n", g.executableName, "(to do)" );
            return 0;
        }

        g.timeout.idle   = 10;
        g.timeout.rescan = 60;

        config = calloc( 1, sizeof(config_t));
        if ( config != NULL ) {
            result = processConfigFiles( config, option.configFile );

            config_destroy( config );
        }

        /* release each non-null entry in argtable[] */
        arg_freetable( argtable, sizeof( argtable ) / sizeof( argtable[ 0 ] ));
    }

    return result;
}

/**
 * @brief
 * @param argc
 * @param argv
 * @return
 */
tError main( int argc, char * argv[] )
{
    tError result;

    g.executableName = strrchr( argv[ 0 ], '/' );
    /* If we found a slash, increment past it. If there's no slash, point at the full argv[0] */
    if ( g.executableName++ == NULL ) {
        g.executableName = argv[ 0 ];
    }
    initLogStuff( g.executableName );

    result = initDaemon();

    if ( result == 0 ) {
        result = processArgs( argc, argv );
    }

    if ( result == 0 ) {
        result = startDaemon();
    }

    return result;
}
