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

using namespace std;

#define DISK_BACKEND_MAX_TABLENAME_SIZE 32
#define DISK_BACKEND_MAX_KEY_SIZE 32

class DiskBackend : public DistStoreBackend
{
    public:
        DiskBackend(const string & doc_root);

        vector<string> getTablenames ();
        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);
        string admin (const string & op, const string & data);
        void validate (const string * tablename, const string * key,
                       const string * value);

    protected:
        static log4cxx::LoggerPtr logger;

        string doc_root;

        void get_dir_pieces (string & d1, string & d2, string & d3, 
                             const string & tablename, const string & key);
        string build_filename(const string & tablename, const string & key);
};

#endif /* HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO */
        
#endif
