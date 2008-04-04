/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef _THRUDOC_LOGGER_H_
#define _THRUDOC_LOGGER_H_

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

#include "EventLog.h"
#include "ThruFileTransport.h"

class FileLogger
{
    public:
        FileLogger (const std::string & log_directory, 
                    const std::string & log_prefix, unsigned int max_ops,
                    unsigned int sync_wait);
        ~FileLogger ();

        void send_log (std::string raw_message);
        void roll_log ();

    private:
        static log4cxx::LoggerPtr logger;

        // this will be used to write to the log file
        boost::shared_ptr<ThruFileWriterTransport> log_transport;
        boost::shared_ptr<EventLogClient> log_client;

        facebook::thrift::concurrency::Mutex log_mutex;
        std::string log_directory;
        std::string log_prefix;
        boost::filesystem::fstream index_file;
        unsigned int num_ops;
        unsigned int max_ops;
        unsigned int sync_wait;

        std::string get_log_filename ();
        void open_log_client (std::string log_filename, bool imediate_sync);
        Event create_event (const std::string &msg);
        void send_nextLog (std::string new_log_filename);
};

#endif
