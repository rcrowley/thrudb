
#include "MyTableHandler.h"
#include "ConfigFile.h"

/*
 * TODO:
 * - input validation, tablenames, key length, value size, etc.
 */

using namespace log4cxx;

LoggerPtr MyTableHandler::logger (Logger::getLogger ("MyTableHandler"));
pthread_key_t MyTableHandler::memcache_key;
bool MyTableHandler::memcached_enabled = false;
string MyTableHandler::memcached_servers;

MyTableHandler::MyTableHandler (boost::shared_ptr<MyTableBackend> backend)
{
    this->backend = backend;
    memcached_servers = ConfigManager->read<string>("MEMCACHED_SERVERS", "");
    LOG4CXX_INFO (logger, string ("MEMCACHED_SERVERS=") + memcached_servers);
    if (!memcached_servers.empty ())
    {
        memcached_enabled = true;
        pthread_key_create (&memcache_key, NULL);
    }
}

memcached_st * MyTableHandler::get_cache ()
{
    memcached_st * cache = (memcached_st*)pthread_getspecific(memcache_key);

    if (cache == NULL)
    {
        LOG4CXX_INFO (logger, "creating memcached_create for thread");

        cache = memcached_create(NULL);

        memcached_server_st * servers =
            memcached_servers_parse((char*)memcached_servers.c_str ());
        memcached_server_push(cache, servers);
        memcached_server_list_free(servers);

        pthread_setspecific(memcache_key, cache);
    }

    return cache;
}

void MyTableHandler::put (const string & tablename, const string & key,
                          const string & value)
{
    LOG4CXX_DEBUG (logger, "put: tablename=" + tablename + ", key=" + key +
                   ", value=" + value);
    this->backend->put (tablename, key, value);
    if (memcached_enabled)
    {
        memcached_st * cache = get_cache ();
        string _key = (tablename + ":" + key);
        memcached_return rc;
        uint16_t opt_flags= 0;
        time_t opt_expires= 0;
        rc = memcached_set (cache, (char*)_key.c_str (), _key.length (),
                            (char*)value.c_str (), value.length (),
                            opt_expires, opt_flags);
    }
}

void MyTableHandler::get (string & _return, const string & tablename,
                          const string & key)
{
    LOG4CXX_DEBUG (logger, "get: tablename=" + tablename + ", key=" + key);
    if (memcached_enabled)
    {
        memcached_st * cache = get_cache ();
        string _key = (tablename + ":" + key);
        memcached_return rc;
        uint16_t opt_flags = 0;
        size_t str_length;
        const char * str = memcached_get(cache, (char*)_key.c_str (),
                                         _key.length (), &str_length,
                                         &opt_flags, &rc);
        if (rc == MEMCACHED_SUCCESS)
        {
            _return = string (str, str_length);
            LOG4CXX_DEBUG (logger, string ("get hit: key=") + key);
        }
        else
        {
            LOG4CXX_DEBUG (logger, string ("get miss: key=") + key);
        }
    }

    if (_return.empty ())
    {
        _return = this->backend->get (tablename, key);
        if (memcached_enabled)
        {
            memcached_st * cache = get_cache ();
            string _key = (tablename + ":" + key);
            memcached_return rc;
            uint16_t opt_flags= 0;
            time_t opt_expires= 0;
            rc = memcached_set (cache, (char*)_key.c_str (), _key.length (), 
                                (char*)_return.c_str (), _return.length (), 
                                opt_expires, opt_flags);
        }
    }
}

void MyTableHandler::remove (const string & tablename, const string & key)
{
    LOG4CXX_DEBUG (logger, "remove: tablename=" + tablename + ", key=" + key);
    this->backend->remove (tablename, key);
    if (memcached_enabled)
    {
        memcached_st * cache = get_cache ();
        string _key = (tablename + ":" + key);
        memcached_return rc;
        time_t opt_expires= 0;
        rc = memcached_delete (cache, (char*)_key.c_str (), _key.length (), 
                               opt_expires);
    }
}

void MyTableHandler::scan (ScanResponse & _return, const string & tablename,
                           const string & seed, int32_t count)
{
    {
        char buf[256];
        sprintf (buf, "scan: tablename=%s, seed=%s, count=%d",
                 tablename.c_str (), seed.c_str (), count);
        LOG4CXX_DEBUG (logger, buf);
    }
    _return = this->backend->scan (tablename, seed, count);
}

void MyTableHandler::admin (string & _return, const string & op, const string & data)
{
    LOG4CXX_DEBUG (logger, "admin: op=" + op + ", data=" + data);
    if (op == "echo")
    {
        // echo is a special admin command that we'll handle at this level,
        // everything else will get passed on down
        _return = data;
    }
    else
    {
        _return = this->backend->admin (op, data);
    }
}
