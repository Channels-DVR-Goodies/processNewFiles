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


/** globals */
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



ssize_t appendString( char ** string, const char * fmt, ... )
        __attribute__ ((format (printf, 2, 3)));

/**
 * @brief
 * @param pid
 * @return
 */
tError createPidFile( pid_t pid )
{
    tError result = 0;

    if ( g.pidFilename != NULL ) {
        FILE * pidFile = fopen( g.pidFilename, "w" );
        if ( pidFile == NULL ) {
            logError( "Error: unable to open %s for writing",
                      g.pidFilename );
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

void removePIDfile()
{
    if ( g.pidFilename != NULL ) {
        unlink( g.pidFilename );
        free((char *)g.pidFilename );
        g.pidFilename = NULL;
    }
}


/**
 * @brief
 */
tError startDaemon( void )
{
    tError result = 0;
    pid_t  pid    = fork();
    switch ( pid ) {
    case -1: // failed
        logError( "Unable to start daemon process" );
        result = -errno;
        break;

    case 0: // child
//        result = initPidFilename( g.executableName );
        logDebug( "pidFilename: \'%s\'", g.pidFilename );
//        return runDaemon();
        break;

    default: // parent
        // everything happens in the child
        break;
    }
    return result;
}

/**
 * @brief
 * Note: this will probably be called from another instance invoked with --kill
 * @return
 */
tError stopDaemon( void )
{
    tError result = 0;
    pid_t  pid    = getDaemonPID();
    logError( "pid: \'%d\'", pid );
    if ( pid < 0 ) {
        result = pid;
    } else if ( killpg( pid, SIGTERM ) == -1 ) {
        logError( "Error: failed to signal the daemon" );
        result = -errno;
    }
    return result;
}

/**
 * @brief
 * @param executableName
 * @return
 */
tError initPidFilename( const char * executableName )
{
    tError result = 0;

    errno         = 0;
    if ( g.pidFilename == NULL ) {
        size_t len = sizeof( "/var/run//.pid" ) + strlen( executableName ) * 2;
        char * pidName = calloc( 1, len );
        if ( pidName == NULL ) {
            logError( "Error: unable to allocate memory  " );
            result = -errno;
        } else {
            snprintf( pidName, len, "/var/run/%s", executableName );
            if ( mkdir( pidName, S_IRWXU) == -1 && errno != EEXIST ) {
                logError( "Error: unable to create directory %s", pidName );
                result = -errno;
            } else {
                errno = 0;
                snprintf( pidName, len,
                          "/var/run/%s/%s.pid",
                          executableName, executableName );
                g.pidFilename = pidName;
            }
        }
    }
    return result;
}

/**
 * @brief
 */
void daemonExit( void )
{
    logInfo( "daemonExit()" );
    removePIDfile();
}

/**
 * @brief
 * @param signum
 */
void daemonSignal( int signum )
{
    logInfo( "daemonSignal(%d)", signum );
    removePIDfile();
}

/**
 * @brief
 * @return
 */
int registerHandlers( void )
{
    pid_t pid = setsid();

    int result = createPidFile( pid );
    if ( result == 0 ) {
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
    }
    if ( result == 0 ) {
        result = eventLoop();
        // eventLoop() is not expected to return in normal operation
    }
    return result;
}

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

int importTree( config_setting_t * group )
{
    if ( config_setting_is_group( group ) ) {
        const char * path;
        const char * exec;
        config_setting_t * member;

        member = config_setting_get_member( group, "path" );
        if ( member != NULL ) {
            path = config_setting_get_string( member );
            if ( path != NULL ) {
                logDebug( "path = \"%s\"", path );
            }
        }

        member = config_setting_get_member( group, "exec" );
        if ( member != NULL ) {
            exec = config_setting_get_string( member );
            if ( exec != NULL ) {
                logDebug( "exec = \"%s\"", exec );
            }
        }
    } else {
        logError( "in %s at line %d: watch entry must be a group", config_setting_source_file( group ), config_setting_source_line( group ) );
    }
    return 0;
}

int importConfig( config_t * config )
{
#ifdef DEBUG
    fprintf( stdout, "###\n" );
    config_write( config, stdout );
#endif
    config_setting_t * setting = config_lookup( config, "watch" );
    if ( setting == NULL ) {
        logError( "unable to find 'watch' element" );
    } else {
        switch ( setting->type ) {
        case CONFIG_TYPE_GROUP:
            importTree( setting );
            break;

        case CONFIG_TYPE_LIST:
            {
                int count = config_setting_length( setting );
                logDebug("type is <list>, of length %d", count );
                for ( int i = 0; i < count; ++i ) {
                    importTree( config_setting_get_elem( setting, i ) );
                }
            }
            break;

        default:
            logError( "'watch' must be either a array or list of arrays" );
        }
    }
    return 0;
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

    /* Note: this clears the config before populating it */

    if ( config_read_file( config, path ) == CONFIG_FALSE ) {
        switch ( config_error_type( config ) )
        {
        case CONFIG_ERR_NONE:
            fprintf(stderr, "no error\n");
            break;

        case CONFIG_ERR_FILE_IO:
            fprintf(stderr, "file I/O error\n");
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
    }

    /* ToDo: loop through any new trees, and add them to our list */

    if ( result == 0 ) {
        importConfig( config );
    }

    return result;
}

tError processConfigFiles( config_t * config, struct arg_file * configFileArg )
{
    tError result = 0;

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
            result = processConfigFile( config,
                                        configFileArg->filename[ i ] );
            if ( result != 0 ) break;
        }
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
    tError       result = 0;
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
            result =  printUsage( argtable );
        } else if ( option.killDaemon->count > 0 ) {  /* ditto for '--kill' */
            result = stopDaemon();
        } else if ( option.version->count > 0 ) {     /* and for '--version' */
            fprintf(  stdout, "%s, version %s\n", g.executableName, "(to do)" );
            return 0;
        } else {
            config = calloc( 1, sizeof(config_t));
            if ( config != NULL ) {
                result = processConfigFiles( config, option.configFile );

                config_destroy( config );
            }
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
    tError exitcode = 0;

    g.executableName = strrchr( argv[ 0 ], '/' );
    /* If we found a slash, increment past it. If there's no slash, point at the full argv[0] */
    if ( g.executableName++ == NULL ) {
        g.executableName = argv[ 0 ];
    }
    initLogStuff( g.executableName );

    registerHandlers();

    exitcode = processArgs( argc, argv );

    return exitcode;
}
