// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DBSTORE_UTIL_H
#define DBSTORE_UTIL_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>

using namespace std;


#define L_ERR   0
#define L_EVENT 1
#define L_DEBUG 2
#define L_FULLDEBUG 3

#define LogLevel L_EVENT

#define dout(n) if (n <= LogLevel) cout<<__PRETTY_FUNCTION__<<" :- "

#endif
