/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef _DISTSTORE_DISK_BACKEND_H_
#define _DISTSTORE_DISK_BACKEND_H_

#if HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO

#include <string>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>

#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "DistStore.h"
#include "DistStoreBackend.h"

#define DISK_BACKEND_MAX_TABLENAME_SIZE 64
#define DISK_BACKEND_MAX_KEY_SIZE 64

class DiskBackend : public DistStoreBackend
{
    public:
        DiskBackend(const std::string & doc_root);

        std::vector<std::string> getTablenames ();
        std::string get (const std::string & tablename,
                         const std::string & key);
        void put (const std::string & tablename, const std::string & key,
                  const std::string & value);
        void remove (const std::string & tablename, const std::string & key);
        diststore::ScanResponse scan (const std::string & tablename,
                                      const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & tablename, const std::string * key,
                       const std::string * value);

    protected:
        static log4cxx::LoggerPtr logger;

        std::string doc_root;

        void get_dir_pieces (std::string & d1, std::string & d2,
                             std::string & d3, const std::string & tablename,
                             const std::string & key);
        std::string build_filename(const std::string & tablename,
                                   const std::string & key);
        std::string build_filename(const std::string & tablename,
                                   const std::string & d1,
                                   const std::string & d2,
                                   const std::string & d3,
                                   const std::string & key);
};

#endif /* HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO */

#endif
