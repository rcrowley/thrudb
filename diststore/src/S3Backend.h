/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef _THRUDOC_S3_BACKEND_H_
#define _THRUDOC_S3_BACKEND_H_

#include <string>
#include <log4cxx/logger.h>

#include "DistStore.h"
#include "DistStoreBackend.h"

class S3Backend : public DistStoreBackend
{
    public:
        S3Backend();

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
};

#endif
