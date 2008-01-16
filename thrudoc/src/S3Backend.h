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

#if HAVE_LIBEXPAT && HAVE_LIBCURL

#include <string>
#include <log4cxx/logger.h>

#include "Thrudoc.h"
#include "ThrudocBackend.h"

class S3Backend : public ThrudocBackend
{
    public:
        S3Backend();

        std::vector<std::string> getTablenames ();
        std::string get (const std::string & tablename,
                         const std::string & key);
        void put (const std::string & tablename, const std::string & key,
                  const std::string & value);
        void remove (const std::string & tablename, const std::string & key);
        thrudoc::ScanResponse scan (const std::string & tablename,
                                    const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & tablename, const std::string * key,
                       const std::string * value);

    protected:
        static log4cxx::LoggerPtr logger;
};

#endif /* HAVE_LIBEXPAT && HAVE_LIBCURL */

#endif
