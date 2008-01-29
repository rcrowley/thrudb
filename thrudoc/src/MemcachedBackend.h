/**
 *
 **/

#ifndef _MEMCACHED_BACKEND_H_
#define _MEMCACHED_BACKEND_H_

#if HAVE_LIBMEMCACHED

#include <memcached.h>
#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

class MemcachedBackend : public ThrudocPassthruBackend
{
    public:
        MemcachedBackend (boost::shared_ptr<ThrudocBackend> backend,
                          const std::string & memcached_servers);
        ~MemcachedBackend ();

        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);

        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    protected:
        memcached_st * get_cache ();

        void cache_put (const std::string & cache_key,
                        const std::string & value);

    private:
        static log4cxx::LoggerPtr logger;

        pthread_key_t memcached_key;
        std::string memcached_servers;
};

#endif /* HAVE_LIBMEMCACHED */

#endif
