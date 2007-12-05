/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDOC_SIMPLE_S3_BACKEND__
#define __THRUDOC_SIMPLE_S3_BACKEND__

#include "ThrudocS3Backend.h"

//Disk backend minus used to realise remote transactions

class ThrudocSimpleS3Backend : public ThrudocS3Backend
{
 public:
    void write (const std::string &id, const std::string &data){
        //Since S3 is avail from all instances we only need the id to update
        //the local bloom
    };

    void remove(const std::string &id ){

    };
};

#endif
