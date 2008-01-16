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

MemcachedBackend::MemcachedBackend (const string & memcached_servers, 
                                    shared_ptr<ThrudocBackend> backend)
{
    LOG4CXX_INFO (logger, string ("MemcachedBackend: memcached_servers=") + 
                  memcached_servers);

    this->backend = backend;
    this->memcached_servers = memcached_servers;

    pthread_key_create (&memcached_key, NULL);
}

MemcachedBackend::~MemcachedBackend ()
{
    // TODO: how do i call memcached_free on each of the thread's objects?
    pthread_key_delete (memcached_key);
}

vector<string> MemcachedBackend::getTablenames ()
{
    return this->backend->getTablenames ();
}

string MemcachedBackend::get (const string & tablename, const string & key )
{
    memcached_st * cache = get_cache ();
    string cache_key = (tablename + ":" + key);
    memcached_return rc;
    uint16_t opt_flags = 0;
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
        sprintf(buf, "memcache get error: tablename=%s, key=%s, strerror=%s", 
                tablename.c_str (), key.c_str (), 
                memcached_strerror(cache, rc));
        LOG4CXX_WARN (logger, buf);
    }

    if (str)
        free (str);

    if (value.empty ())
    {
        value = this->backend->get (tablename, key);
        cache_put (cache_key, value);
    }

    return value;
}

void MemcachedBackend::put (const string & tablename, const string & key, 
                            const string & value)
{
    this->backend->put (tablename, key, value);
    string cache_key = (tablename + ":" + key);
    cache_put (cache_key, value);
}

void MemcachedBackend::remove (const string & tablename, const string & key )
{
    this->backend->remove (tablename, key);

    memcached_st * cache = get_cache ();
    string cache_key = (tablename + ":" + key);
    memcached_return rc;
    time_t opt_expires= 0;
    rc = memcached_delete (cache, (char*)cache_key.c_str (), 
                           cache_key.length (), opt_expires);
    if (rc != MEMCACHED_SUCCESS)
    {
        char buf[256];
        sprintf(buf, "memcache remove error: tablename=%s, key=%s, strerror=%s", 
                tablename.c_str (), key.c_str (), 
                memcached_strerror(cache, rc));
        LOG4CXX_WARN (logger, buf);
    }
}

ScanResponse MemcachedBackend::scan (const string & tablename,
                                     const string & seed, int32_t count)
{
    return this->backend->scan (tablename, seed, count);
}

string MemcachedBackend::admin (const string & op, const string & data)
{
    return this->backend->admin (op, data);
}

void MemcachedBackend::validate (const string & tablename, const string * key, 
                                 const string * value)
{
    this->backend->validate (tablename, key, value);
    if (tablename.find (":") != string::npos)
    {
        ThrudocException e;
        e.what = "invalid tablename";
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
