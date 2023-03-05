//
// Created by paul on 12/10/22.
//

#ifndef PROCESSNEWFILES__INOTIFY_H_
#define PROCESSNEWFILES__INOTIFY_H_

tError    processInotifyEvents( tWatchedTree * watchedTree );
tError    registerForInotifyEvents( tWatchedTree * watchedTree );
void      resetExpiration(tFSNode * node, tExpiredReason reason );
time_t    nextExpiration( void );
tFSNode * fsNodeFromPath( tWatchedTree * watchedTree, const char * fullPath, tFSNodeType type );
void      forgetWatch(const tFSNode *fsNode);

#endif //PROCESSNEWFILES__INOTIFY_H_
