/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef _THRUDOC_BACKEND_H_
#define _THRUDOC_BACKEND_H_

#include <string>
#include <memcache++.h>
#include <boost/shared_ptr.hpp>

/**
 * API for storage backends
 *
 * Disk / Berkeley DB / Mysql / Amazon S3
 *
 **/

class ThrudocBackend
{
 public:
    virtual ~ThrudocBackend(){};

    virtual std::string read  (const std::string &id ) = 0;
    virtual void write (const std::string &id, const std::string &data) = 0;
    virtual void remove(const std::string &id ) = 0;

    void setCacheHandle( boost::shared_ptr<Memcache> memd ){ this->memd = memd; };

 protected:
    boost::shared_ptr<Memcache> memd;

};


#endif
