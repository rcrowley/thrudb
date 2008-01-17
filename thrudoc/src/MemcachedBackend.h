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
#include "ThrudocBackend.h"

class MemcachedBackend : public ThrudocBackend
{
    public:
        MemcachedBackend (const std::string & memcached_servers,
                          boost::shared_ptr<ThrudocBackend> backend);
        ~MemcachedBackend ();

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
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
        boost::shared_ptr<ThrudocBackend> backend;
};

#endif /* HAVE_LIBMEMCACHED */

#endif
