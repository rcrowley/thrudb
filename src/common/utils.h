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
#include <openssl/md5.h>

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

    std::string::size_type pos  = 0;
    std::string::size_type next = 0;

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


//MD5 right padded to be same size as UUID
inline bool isMD5( const char *id ){

    if(strlen(id) != 36)
        return false;

    //lame check
    const char *end = id+32;

    if(strcmp(end,"1977") != 0)
        return false;


    for(int i=0; i<32; i++){
        char c = id[i];

        if( !(isdigit(c)||c=='a'||c=='b'||c=='c'||c=='d'||c=='e'||c=='f'))
            return false;
    }


    return true;
}


inline std::string generateMD5( const std::string &id ){

    std::string md5hex;

    char hex[3];
    memset(hex,0,sizeof(hex));

    unsigned char md5[16];
    memset(md5,0,sizeof(md5));

    MD5((const unsigned char *)id.c_str(),id.length(),md5);

    for(int i=0; i<16; i++){
        sprintf(hex,"%02x", md5[i]);

        md5hex += std::string(hex);
    }

    //rpad to 36
    md5hex += std::string("1977");

    return md5hex;
}

#endif
