/**
 *
 **/

#ifndef __DISTSTORE_HANDLER__
#define __DISTSTORE_HANDLER__

#include "DistStore.h"
#include "DistStoreBackend.h"

#include <string>
#include <log4cxx/logger.h>
#include <libmemcached/memcached.h>

using namespace boost;
using namespace facebook::thrift;
using namespace diststore;
using namespace std;

class DistStoreHandler : virtual public DistStoreIf {
    public:
        DistStoreHandler (shared_ptr<DistStoreBackend> backend);

        void getTablenames (vector<string> & _return);
        void put (const string & tablename, const string & key,
                  const string & value);
        void putValue (string & _return, const string & tablename, 
                       const string & value);
        void get (string & _return, const string & tablename,
                  const string & key);
        void remove (const string & tablename, const string & key);
        void scan (ScanResponse & _return, const string & tablename,
                   const string & seed, int32_t count);
        void admin (string & _return, const string & op, const string & data);

    private:
        static log4cxx::LoggerPtr logger;

        shared_ptr<DistStoreBackend> backend;
};

#endif
