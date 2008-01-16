/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/


#ifndef __QUEUE__H__
#define __QUEUE__H__

#include <set>
#include <string>
#include <deque>
#include <log4cxx/logger.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/transport/TFileTransport.h>
#include <thrift/transport/TTransportUtils.h>

#include "Thruqueue.h"
#include "Thruqueue_types.h"
#include "Thruqueue_constants.h"

#include "QueueLog.h"

class Queue
{
 public:
    Queue(const std::string name, bool unique = false);

    void         enqueue(const Thruqueue::QueueMessage &mess);
    void         enqueue(const std::string &mess, bool priority = false);
    std::string  dequeue();
    std::string  peek();

    unsigned int length();
    void         flush();

 private:
    void pruneLogFile();
    void bufferMessagesFromLog();


    facebook::thrift::concurrency::Mutex mutex;

    std::set<std::string>   unique_keys;

    std::deque<Thruqueue::QueueMessage> queue;
    std::deque<std::string>             pruning_queue;
    std::deque<std::string>             priority_queue;

    std::string             queue_name;
    bool                    is_unique;
    std::string             queue_log_file;
    unsigned int            queue_length;


    bool is_pruning;
    boost::shared_ptr<facebook::thrift::transport::TFileTransport> queue_log;
    boost::shared_ptr<facebook::thrift::transport::TFileTransport> queue_log_reader;

    boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer>  transport;
    boost::shared_ptr<Thruqueue::QueueLogClient>                   queue_log_client;

    boost::shared_ptr<facebook::thrift::transport::TFileProcessor> queue_log_processor;


    unsigned int                msg_buffer_size;
    static log4cxx::LoggerPtr   logger;
};

#endif
