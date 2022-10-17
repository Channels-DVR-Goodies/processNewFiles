//
// Created by paul on 10/1/22.
//

#ifndef PROCESSNEWFILES__EVENTS_H_
#define PROCESSNEWFILES__EVENTS_H_

typedef uint32_t    tCookie;
typedef int         tWatchID;
typedef int         tNFTWresult;

typedef enum {
    kRescan = 0, kNew, kModified, kMoved
} tExpiredAction;



typedef struct {
    const char *  path;
    size_t    	  pathLen;
    tFileDscr 	  fd;
} tDir;


#if 0
typedef int tPipe[2];
enum {
    kPipeReadFD = 0, kPipeWriteFD = 1
};
#endif

tError  eventLoop( void );

int     stopDaemon( void );

tError  watchTree( const char * dir );

#endif //PROCESSNEWFILES__EVENTS_H_
