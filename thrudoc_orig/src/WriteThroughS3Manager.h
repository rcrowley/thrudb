/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __WRITE_THROUGH_S3_MANAGER__
#define __WRITE_THROUGH_S3_MANAGER__

#include <string>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <log4cxx/logger.h>

/**
 *This class syncs writes to S3 on a separate thread by tailing the redo log
 *On startup it will make sure all the writes in the redo log have propagated
 *to S3 before initializing.
 **/
class _WriteThroughS3Manager : public facebook::thrift::concurrency::Runnable
{
 public:

    void   startup();
    void   destroy();

    void   run();

    void   add( const std::string &id );

    static _WriteThroughS3Manager* instance();
private:

    _WriteThroughS3Manager() : started(false){};

    bool   started;
    static log4cxx::LoggerPtr logger;
    static _WriteThroughS3Manager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;
};


//Singleton shortcut
#define WriteThroughS3Manager _WriteThroughS3Manager::instance()


#endif
