/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __UTILS_H__
#define __UTILS_H__

#include <sys/stat.h>
#include <string>
#include <vector>
#include <iostream>

inline bool file_exists( std::string filename )
{
    struct stat buffer;
    if ( stat( filename.c_str(), &buffer ) == 0 ) return true ;
    return false ;
}


inline bool file_exists( std::string filename, struct stat &buffer )
{
    if ( stat( filename.c_str(), &buffer ) == 0 ) return true ;
    return false ;
}

inline bool directory_exists( std::string filename )
{
    struct stat buffer;
    if ( stat( filename.c_str(), &buffer ) == 0 ){
        if(buffer.st_mode & S_IFDIR)
            return true;

    }
    return false ;
}

#endif
