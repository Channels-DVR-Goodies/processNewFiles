//
// Created by paul on 10/2/22.
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

#include <uthash.h>
#include <utlist.h>


typedef unsigned long   tHash;
typedef int             tError;
typedef int		tFileDscr;

typedef struct {
    const char *  executableName;

    char *  pidFilename;

    struct {
        time_t    idle;
        time_t    rescan;
    } timeout;

    bool          running;
} tGlobals;

extern tGlobals g;

#endif //PROCESSNEWFILES__PROCESSNEWFILES_H_
