//
// Created by paul on 10/19/22.
//

#include "events.h"

#ifndef PROCESSNEWFILES__RESCAN_H_
#define PROCESSNEWFILES__RESCAN_H_

tError rescanTree( tFSNode * watchedTree );
tError rescanAllTrees( void );

#endif //PROCESSNEWFILES__RESCAN_H_
