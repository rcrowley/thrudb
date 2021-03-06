#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "MemcachedBackend.h"

#if HAVE_LIBMEMCACHED

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

// private
LoggerPtr MemcachedBackend::logger (Logger::getLogger ("MemcachedBackend"));

MemcachedBackend::MemcachedBackend (shared_ptr<ThrudocBackend> backend,
                                    const string & memcached_servers)
{
    LOG4CXX_INFO (logger, string ("MemcachedBackend: memcached_servers=") + 
                  memcached_servers);

    this->set_backend (backend);
    this->memcached_servers = memcached_servers;

    pthread_key_create (&memcached_key, NULL);
}

MemcachedBackend::~MemcachedBackend ()
{
    // TODO: how do i call memcached_free on each of the thread's objects?
    pthread_key_delete (memcached_key);
}

string MemcachedBackend::get (const string & bucket, const string & key )
{
    memcached_st * cache = get_cache ();
    string cache_key = (bucket + ":" + key);
    memcached_return rc;
    uint32_t opt_flags = 0;
    size_t str_length;
    char * str = memcached_get(cache, (char*)cache_key.c_str (),
                               cache_key.length (), &str_length,
                               &opt_flags, &rc);
    string value;
    if (rc == MEMCACHED_SUCCESS)
    {
        value = string (str, str_length);
        LOG4CXX_DEBUG (logger, string ("get hit: key=") + key);
    }
    else if (rc == MEMCACHED_NOTFOUND)
    {
        LOG4CXX_DEBUG (logger, string ("get miss: key=") + key);
    }
    else
    {
        char buf[256];
        sprintf(buf, "memcache get error: bucket=%s, key=%s, strerror=%s", 
                bucket.c_str (), key.c_str (), 
                memcached_strerror(cache, rc));
        LOG4CXX_WARN (logger, buf);
    }

    if (str)
        free (str);

    if (value.empty ())
    {
        value = this->get_backend ()->get (bucket, key);
        cache_put (cache_key, value);
    }

    return value;
}

void MemcachedBackend::put (const string & bucket, const string & key, 
                            const string & value)
{
    this->get_backend ()->put (bucket, key, value);
    string cache_key = (bucket + ":" + key);
    cache_put (cache_key, value);
}

void MemcachedBackend::remove (const string & bucket, const string & key )
{
    this->get_backend ()->remove (bucket, key);

    memcached_st * cache = get_cache ();
    string cache_key = (bucket + ":" + key);
    memcached_return rc;
    time_t opt_expires= 0;
    rc = memcached_delete (cache, (char*)cache_key.c_str (), 
                           cache_key.length (), opt_expires);
    if (rc != MEMCACHED_SUCCESS)
    {
        char buf[256];
        sprintf(buf, "memcache remove error: bucket=%s, key=%s, strerror=%s", 
                bucket.c_str (), key.c_str (), 
                memcached_strerror(cache, rc));
        LOG4CXX_WARN (logger, buf);
    }
}

void MemcachedBackend::validate (const string & bucket, const string * key, 
                                 const string * value)
{
    this->get_backend ()->validate (bucket, key, value);

    if (bucket.find (":") != string::npos)
    {
        ThrudocException e;
        e.what = "invalid bucket";
        throw e;
    }
}

memcached_st * MemcachedBackend::get_cache ()
{
    memcached_st * cache = (memcached_st*)pthread_getspecific(memcached_key);

    if (cache == NULL)
    {
        LOG4CXX_INFO (logger, "creating memcached_create for thread");

        cache = memcached_create(NULL);

        memcached_server_st * servers =
            memcached_servers_parse((char*)memcached_servers.c_str ());
        memcached_server_push(cache, servers);
        memcached_server_list_free(servers);

        pthread_setspecific(memcached_key, cache);
    }

    return cache;
}

void MemcachedBackend::cache_put (const string & cache_key, 
                                  const string & value)
{
    memcached_st * cache = get_cache ();
    memcached_return rc;
    uint16_t opt_flags= 0;
    time_t opt_expires= 0;
    rc = memcached_set (cache, (char*)cache_key.c_str (), cache_key.length (),
                        (char*)value.c_str (), value.length (),
                        opt_expires, opt_flags);
    if (rc != MEMCACHED_SUCCESS)
    {
        char buf[256];
        sprintf(buf, "memcache put error: cache_key=%s, strerror=%s", 
                cache_key.c_str (), memcached_strerror(cache, rc));
        LOG4CXX_WARN (logger, buf);
    }
}

#endif /* HAVE_LIBMEMCACHED */
