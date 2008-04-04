/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef _THRUDEX_LOG_BACKEND_H_
#define _THRUDEX_LOG_BACKEND_H_

#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>
#include <log4cxx/logger.h>
#include <string>
#include <thrift/concurrency/Mutex.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDenseProtocol.h>
#include <thrift/transport/TFileTransport.h>
#include <thrift/transport/TTransportUtils.h>

#include "ThrudexPassthruBackend.h"

#include <EventLog.h>
#include <FileLogger.h>

#define LOG_FILE_PREFIX "thrudex-log."

class LogBackend : public ThrudexPassthruBackend
{
    public:
        LogBackend (boost::shared_ptr<ThrudexBackend> backend,
                    const std::string &log_directory, unsigned int max_ops,
                    unsigned int sync_wait);

        void put(const thrudex::Document &d);
        void remove(const thrudex::Element &e);

        std::vector<thrudex::ThrudexException> putList
            (const std::vector<thrudex::Document> &documents);
        std::vector<thrudex::ThrudexException> removeList
            (const std::vector<thrudex::Element> &elements);

        std::string admin(const std::string &op, const std::string &data);

    private:
        static log4cxx::LoggerPtr logger;

        // this will be used to create the event message
        boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer> msg_transport;
        boost::shared_ptr<thrudex::ThrudexClient> msg_client;
        Event create_event (const std::string &msg);

        FileLogger * file_logger;
};

#endif
