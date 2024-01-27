//
// Created by Paul Chambers on 10/2/22.
//

#ifndef PROCESSNEWFILES__PROCESSNEWFILES_H_
#define PROCESSNEWFILES__PROCESSNEWFILES_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>
#include <errno.h>
#include <linux/limits.h>
#include <signal.h>

#include <string.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <ftw.h>

typedef unsigned long   tHash;
typedef int             tError;
typedef int     		tFileDscr;

#include "list.h"
#include "radixTree.h"
#include "hashmap.h"
#include "logStuff.h"

//typedef struct nextNode tFSNode;
typedef struct sFSNode tFSNode;

typedef struct {
    const char *  executableName;   /* basename used to invoke us */

    char *        pidFilename;

    struct {
        time_t    idle;
        time_t    rescan;
    } timeout;

    tListRoot *  expiringList;      /* linked list of nodes waiting to expire, ordered by ascending expiration time */
    tListRoot *  readyList;         /* linked list of nodes ready to be executed */
    int          readyCount;        /* number of nodes currently in the list. We only maintain a limited number at
                                       any point in time, otherwise there could be tens of thousands of nodes made
                                       'ready' nodes from the first scan of a large hierarchy */
    tListRoot *  executingList;     /* linked list of nodes currently executing. If it returns a non-zero exit code,
                                       it'll be put back on the expiringList, and be retried after am 'idle' delay */

    tRadixTree * pathTree;          /* radix tree of full paths */

} tGlobals;

extern tGlobals g;

#endif //PROCESSNEWFILES__PROCESSNEWFILES_H_
