/**
 *
 **/

#ifndef _MEMCACHED_BACKEND_H_
#define _MEMCACHED_BACKEND_H_

#if HAVE_LIBMEMCACHED

#include <libmemcached/memcached.h>
#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "DistStore.h"
#include "DistStoreBackend.h"

using namespace boost;
using namespace log4cxx;
using namespace diststore;
using namespace std;

class MemcachedBackend : public DistStoreBackend
{
    public:
        MemcachedBackend (const string & memcached_servers, 
                          shared_ptr<DistStoreBackend> backend);
        ~MemcachedBackend ();

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
        memcached_st * get_cache ();

        void cache_put (const string & cache_key, const string & value);

    private:
        static log4cxx::LoggerPtr logger;
        static pthread_key_t memcached_key;
        
        string memcached_servers;
        shared_ptr<DistStoreBackend> backend;
};

#endif /* HAVE_LIBMEMCACHED */

#endif
