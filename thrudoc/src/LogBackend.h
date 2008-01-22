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

#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TFileTransport.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDenseProtocol.h>

#include "ThrudocBackend.h"
#include "Redo.h"

class LogBackend : public ThrudocBackend
{
 public:
    LogBackend( const std::string &log_directory, boost::shared_ptr<ThrudocBackend> backend );
    ~LogBackend();

    std::vector<std::string> getBuckets();
    std::string get (const std::string & bucket,
                     const std::string & key);

    void put (const std::string & bucket, const std::string & key,
              const std::string & value);

    void remove (const std::string & bucket, const std::string & key);

    thrudoc::ScanResponse scan (const std::string & bucket,
                                  const std::string & seed, int32_t count);

    std::string admin (const std::string & op, const std::string & data);

    void validate (const std::string & bucket, const std::string * key,
                   const std::string * value);

 private:
    RedoMessage createLogMessage(const std::string &msg);

    static log4cxx::LoggerPtr logger;
    boost::shared_ptr<ThrudocBackend> backend;

    //This will be used to create the log msg
    boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer>  transport;
    boost::shared_ptr<thrudoc::ThrudocClient>                      faux_client;

    //for logfile
    std::string log_directory;
    boost::shared_ptr<facebook::thrift::transport::TFileTransport> thrudoc_log;
    boost::shared_ptr<RedoClient>                                  log_client;
};

#endif
