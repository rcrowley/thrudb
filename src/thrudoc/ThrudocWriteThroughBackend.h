/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __THRUDOC_WRITE_THROUGH_BACKEND__
#define __THRUDOC_WRITE_THROUGH_BACKEND__

/**
 *This backend is a hybrid of the Disk and S3 backends
 *Suggested by Ilya Grigorik should provide the best mix of performance
 *and reliability.
 *
 *Described as "Full write through storage" writes will happen like so:
 *
 * doc -> disk -> memcached -> redo log -> s3
 *
 *And reads:
 * id -> bloom filter -> memcached -> disk -> s3
 *
 **/

#include <string>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>

#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "Thrudoc.h"
#include "ThrudocDiskBackend.h"
#include "ThrudocS3Backend.h"

class ThrudocWriteThroughBackend : public ThrudocBackend
{
 public:

    ThrudocWriteThroughBackend();

    std::string read   (const std::string &id );
    virtual void write (const std::string &id, const std::string &data);
    virtual void remove(const std::string &id );

 protected:

    ThrudocDiskBackend disk;
    ThrudocS3Backend   s3;

    static log4cxx::LoggerPtr logger;
};



#endif
