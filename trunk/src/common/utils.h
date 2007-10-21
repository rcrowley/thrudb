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
#include <uuid/uuid.h>

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

inline std::vector<std::string> split(std::string str, std::string delim)
{
    std::vector<std::string> vec;

    unsigned int pos  = 0;
    unsigned int next = 0;

    while( (next = str.substr(pos).find(delim) ) != std::string::npos ){

        vec.push_back( str.substr(pos,next) );

        pos += next + 1;
    }

    if( pos == 0 || pos <= str.size())
        vec.push_back( str.substr(pos) );

    return vec;
}


inline std::string generateUUID()
{
    uuid_t uuid;
    uuid_generate(uuid);

    char uuid_str[37];

    uuid_unparse_lower(uuid, uuid_str);

    return uuid_str;
}

#endif
