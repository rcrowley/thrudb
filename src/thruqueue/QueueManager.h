/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __QUEUE_MANAGER__
#define __QUEUE_MANAGER__

#include <string>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <log4cxx/logger.h>

#include "Queue.h"

/**
 *Holds named array of active queues.
 *
 **/
class _QueueManager : public facebook::thrift::concurrency::Runnable
{
 public:
    void   startup();

    void   createQueue ( const std::string &id, bool unique = false );
    void   destroyQueue( const std::string &id );
    boost::shared_ptr<Queue> getQueue( const std::string &id );

    static _QueueManager* instance();

 private:
    _QueueManager() : started(false){};
    void   run();
    facebook::thrift::concurrency::Mutex mutex;

    std::map<std::string, boost::shared_ptr<Queue> > queue_cache;

    bool   started;
    static log4cxx::LoggerPtr logger;
    static _QueueManager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;
};

//Singleton shortcut
#define QueueManager _QueueManager::instance()

#endif
