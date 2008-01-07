
#ifdef HAVE_CONFIG_H
#include "diststore_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "DistStoreHandler.h"
#include "ConfigFile.h"

#if HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

using namespace boost;
using namespace log4cxx;

LoggerPtr DistStoreHandler::logger (Logger::getLogger ("DistStoreHandler"));

DistStoreHandler::DistStoreHandler (shared_ptr<DistStoreBackend> backend)
{
    this->backend = backend;
}

void DistStoreHandler::getTablenames (vector<string> & _return)
{
    LOG4CXX_DEBUG (logger, "getTablenames: ");
    _return = this->backend->getTablenames ();
}

void DistStoreHandler::put (const string & tablename, const string & key,
                            const string & value)
{
    LOG4CXX_DEBUG (logger, "put: tablename=" + tablename + ", key=" + key +
                   ", value=" + value);
    this->backend->validate (tablename, &key, &value);
    this->backend->put (tablename, key, value);
}

void DistStoreHandler::putValue (string & _return, const string & tablename, 
                                 const string & value)
{
#if HAVE_LIBUUID
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    _return = string (uuid_str);
#else
    DistStoreException e;
    e.what = "putValue uuid generation not built";
    throw e;
#endif
    LOG4CXX_DEBUG (logger, "putValue: tablename=" + tablename + ", value=" + 
                   value + ", uuid=" + _return);
    this->backend->put (tablename, _return, value);
}

void DistStoreHandler::get (string & _return, const string & tablename,
                            const string & key)
{
    LOG4CXX_DEBUG (logger, "get: tablename=" + tablename + ", key=" + key);
    this->backend->validate (tablename, &key, NULL);
    _return = this->backend->get (tablename, key);
}

void DistStoreHandler::remove (const string & tablename, const string & key)
{
    LOG4CXX_DEBUG (logger, "remove: tablename=" + tablename + ", key=" + key);
    this->backend->validate (tablename, &key, NULL);
    this->backend->remove (tablename, key);
}

void DistStoreHandler::scan (ScanResponse & _return, const string & tablename,
                             const string & seed, int32_t count)
{
    {
        char buf[256];
        sprintf (buf, "scan: tablename=%s, seed=%s, count=%d",
                 tablename.c_str (), seed.c_str (), count);
        LOG4CXX_DEBUG (logger, buf);
    }
    this->backend->validate (tablename, NULL, NULL);
    _return = this->backend->scan (tablename, seed, count);
}

void DistStoreHandler::admin (string & _return, const string & op, const string & data)
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
