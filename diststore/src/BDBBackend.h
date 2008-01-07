/**
 *
 **/

#ifndef _THRUDOC_BDB_BACKEND_H_
#define _THRUDOC_BDB_BACKEND_H_

#if HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM

#include <string>
#include <log4cxx/logger.h>
#include <db_cxx.h>

#include "DistStore.h"
#include "DistStoreBackend.h"

#include <thrift/concurrency/Mutex.h>

using namespace facebook::thrift::concurrency;

class BDBBackend : public DistStoreBackend
{
    public:
        BDBBackend (const string & bdb_root, const int & thread_count);
        ~BDBBackend ();

        vector<string> getTablenames ();
        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);
        string admin (const string & op, const string & data);
        void validate (const string & tablename, const string * key,
                       const string * value);

    protected:
        static log4cxx::LoggerPtr logger;
        static Mutex db_ops_mutex;

        string bdb_home;
        DbEnv * db_env;
        map<string, Db *> dbs;

        Db * get_db (const string & tablename);
};

#endif /* HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM */

#endif
