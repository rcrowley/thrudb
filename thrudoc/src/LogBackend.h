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

#include "ThrudocPassthruBackend.h"
#include "EventLog.h"
#include "ThruFileTransport.h"

#define LOG_FILE_PREFIX "thrudoc-log."

class LogBackend : public ThrudocPassthruBackend
{
    public:
        LogBackend (boost::shared_ptr<ThrudocBackend> backend,
                    const std::string &log_directory, unsigned int max_ops,
                    unsigned int sync_wait);
        ~LogBackend ();

        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);

        void remove (const std::string & bucket, const std::string & key);

        std::string admin (const std::string & op, const std::string & data);

        std::vector<thrudoc::ThrudocException> putList
            (const std::vector<thrudoc::Element> & elements);
        std::vector<thrudoc::ThrudocException> removeList
            (const std::vector<thrudoc::Element> & elements);

    private:
        static log4cxx::LoggerPtr logger;

        // this will be used to create the event message
        boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer> msg_transport;
        boost::shared_ptr<thrudoc::ThrudocClient> msg_client;

        // this will be used to write to the log file
        boost::shared_ptr<ThruFileWriterTransport> log_transport;
        boost::shared_ptr<EventLogClient> log_client;

        facebook::thrift::concurrency::Mutex log_mutex;
        std::string log_directory;
        boost::filesystem::fstream index_file;
        unsigned int num_ops;
        unsigned int max_ops;
        unsigned int sync_wait;

        std::string get_log_filename ();
        void open_log_client (std::string log_filename, bool imediate_sync);
        void roll_log ();
        Event create_event (const std::string &msg);
        void send_log (std::string raw_message);
        void send_nextLog (std::string new_log_filename);
};

#endif
