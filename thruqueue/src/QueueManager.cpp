
/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "QueueManager.h"
#include "Thruqueue.h"
#include "ConfigFile.h"
#include "utils.h"
#include <stdexcept>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

using namespace std;
using namespace boost;
using namespace facebook::thrift;
using namespace log4cxx;
using namespace thruqueue;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;


_QueueManager* _QueueManager::pInstance = 0;
Mutex          _QueueManager::_mutex    = Mutex();
LoggerPtr      _QueueManager::logger(Logger::getLogger("QueueManager"));

struct null_deleter
{
    void operator()(void const *) const {}
};

_QueueManager *_QueueManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _QueueManager();

            //Validate current state
            pInstance->startup();

            shared_ptr<PosixThreadFactory> threadFactory =
                shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

            shared_ptr<Thread> thread =
                threadFactory->newThread(shared_ptr<_QueueManager>(pInstance,null_deleter()));

            thread->start();
        }
    }

    return pInstance;
}

void _QueueManager::startup()
{
    if(started)
        return;

    string doc_root    = ConfigManager->read<string>("DOC_ROOT");

    if( !directory_exists(doc_root) )
        throw std::runtime_error("DOC_ROOT is not valid (check config)");

    started = true;

}

void _QueueManager::run()
{
    while(true){

        sleep(10);

    }
}


void  _QueueManager::createQueue ( const std::string &id, bool unique )
{
    Guard g(mutex);

    if( queue_cache.count(id) == 0 )
        queue_cache[id] = shared_ptr<Queue>(new Queue(id,unique));

}

void  _QueueManager::destroyQueue( const std::string &id )
{
    Guard g(mutex);

    if( queue_cache.count(id) > 0 )
        queue_cache.erase(id);

}

shared_ptr<Queue> _QueueManager::getQueue( const std::string &id )
{
    Guard g(mutex);

    if( queue_cache.count(id) > 0 )
        return queue_cache[id];

    ThruqueueException e;
    e.what = "queue not found: "+id;

    throw e;
}
