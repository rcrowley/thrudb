/**
 *
 **/

#ifndef _THRUDOC_BDB_BACKEND_H_
#define _THRUDOC_BDB_BACKEND_H_

#if HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM

#include <string>
#include <log4cxx/logger.h>
#include <db_cxx.h>

#include "Thrudoc.h"
#include "ThrudocBackend.h"

class BDBBackend : public ThrudocBackend
{
    public:
        BDBBackend (const std::string & bdb_root, const int & thread_count);
        ~BDBBackend ();

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
		void append(const std::string & bucket, const std::string & key,
			const std::string & value);
        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed,
                                    int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    protected:
        static log4cxx::LoggerPtr logger;

        std::string bdb_home;
        DbEnv * db_env;
        std::map<std::string, Db *> dbs;

        Db * get_db (const std::string & bucket);
};

#endif /* HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM */

#endif
