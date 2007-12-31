/**
 *
 **/

#ifndef _MEMCACHED_BACKEND_H_
#define _MEMCACHED_BACKEND_H_

#include <libmemcached/memcached.h>
#include <log4cxx/logger.h>
#include <set>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include "MyTable.h"
#include "MyTableBackend.h"

using namespace boost;
using namespace log4cxx;
using namespace mytable;
using namespace std;

class MemcachedBackend : public MyTableBackend
{
    public:
        MemcachedBackend (string memcached_servers, 
                          shared_ptr<MyTableBackend> backend);
        ~MemcachedBackend ();

        vector<string> getTablenames ();
        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);

        string admin (const string & op, const string & data);

    protected:
        memcached_st * get_cache ();

        void cache_put (const string & cache_key, const string & value);

    private:
        static log4cxx::LoggerPtr logger;
        static pthread_key_t memcached_key;
        static string memcached_servers;

        shared_ptr<MyTableBackend> backend;
};

#endif
