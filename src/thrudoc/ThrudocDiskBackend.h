/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef _THRUDOC_DISK_BACKEND_H_
#define _THRUDOC_DISK_BACKEND_H_

#include <string>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>

#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "Thrudoc.h"
#include "ThrudocBackend.h"

class ThrudocDiskBackend : public ThrudocBackend
{
 public:
    ThrudocDiskBackend();

    std::string read  (const std::string &id );
    virtual void write (const std::string &id, const std::string &data);
    virtual void remove(const std::string &id );

 protected:

    void _write (const std::string &id, const std::string &data);
    void _remove (const std::string &id);

    boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer>  transport;
    boost::shared_ptr<thrudoc::ThrudocClient>                        faux_client;

    std::string build_filename(const std::string &id);

    std::string doc_root;

    static log4cxx::LoggerPtr logger;

};

inline std::string ThrudocDiskBackend::build_filename(const std::string &id)
{
    return doc_root+"/"+id.substr(0,2)+"/"+id.substr(2,2)+"/"+id.substr(4,2)+"/"+id;
}

#endif
