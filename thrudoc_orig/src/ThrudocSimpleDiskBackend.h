/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDOC_SIMPLE_DISK_BACKEND__
#define __THRUDOC_SIMPLE_DISK_BACKEND__

#include "ThrudocDiskBackend.h"

//Disk backend minus used to realise remote transactions

class ThrudocSimpleDiskBackend : public ThrudocDiskBackend
{
 public:
    void write (const std::string &id, const std::string &data){
        this->_write(id,data);
    };

    void remove(const std::string &id ){
        this->_remove(id);
    };

};

#endif
