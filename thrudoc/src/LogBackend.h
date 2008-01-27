/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef _THRUDOC_LOG_BACKEND_H_
#define _THRUDOC_LOG_BACKEND_H_


#include <string>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>

#include <thrift/concurrency/Mutex.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDenseProtocol.h>
#include <thrift/transport/TFileTransport.h>
#include <thrift/transport/TTransportUtils.h>

#include "ThrudocBackend.h"
#include "EventLog.h"

class LogBackend : public ThrudocBackend
{
    public:
        LogBackend (boost::shared_ptr<ThrudocBackend> backend,
                    const std::string &log_directory, unsigned int max_ops);
        ~LogBackend ();

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);

        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);

        void remove (const std::string & bucket, const std::string & key);

        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed, int32_t count);

        std::string admin (const std::string & op, const std::string & data);

        std::vector<thrudoc::ThrudocException> putList
            (const std::vector<thrudoc::Element> & elements);
        std::vector<thrudoc::ThrudocException> removeList
            (const std::vector<thrudoc::Element> & elements);

        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    private:
        static log4cxx::LoggerPtr logger;

        boost::shared_ptr<ThrudocBackend> backend;

        // this will be used to create the event message
        boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer> msg_transport;
        boost::shared_ptr<thrudoc::ThrudocClient> msg_client;

        // this will be used to write to the log file
        boost::shared_ptr<facebook::thrift::transport::TFileTransport> log_transport;
        boost::shared_ptr<EventLogClient> log_client;
        facebook::thrift::concurrency::Mutex log_mutex;
        std::string log_directory;
        unsigned int num_ops;
        unsigned int max_ops;

        Event createEvent (const std::string &msg);
        std::string get_log_filename ();
        void send_message (std::string raw_message);
};

#endif
