
#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "ThrudocHandler.h"

#if HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

LoggerPtr ThrudocHandler::logger (Logger::getLogger ("ThrudocHandler"));

ThrudocHandler::ThrudocHandler (shared_ptr<ThrudocBackend> backend)
{
    this->backend = backend;
}

void ThrudocHandler::getTablenames (vector<string> & _return)
{
    LOG4CXX_DEBUG (logger, "getTablenames: ");
    _return = this->backend->getTablenames ();
}

void ThrudocHandler::put (const string & tablename, const string & key,
                          const string & value)
{
    LOG4CXX_DEBUG (logger, "put: tablename=" + tablename + ", key=" + key +
                   ", value=" + value);
    this->backend->validate (tablename, &key, &value);
    this->backend->put (tablename, key, value);
}

void ThrudocHandler::putValue (string & _return, const string & tablename,
                               const string & value)
{
#if HAVE_LIBUUID
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    _return = string (uuid_str);
    LOG4CXX_DEBUG (logger, "putValue: tablename=" + tablename + ", value=" +
                   value + ", uuid=" + _return);
    this->backend->put (tablename, _return, value);
#else
    ThrudocException e;
    e.what = "putValue uuid generation not built";
    throw e;
#endif
}

void ThrudocHandler::get (string & _return, const string & tablename,
                          const string & key)
{
    LOG4CXX_DEBUG (logger, "get: tablename=" + tablename + ", key=" + key);
    this->backend->validate (tablename, &key, NULL);
    _return = this->backend->get (tablename, key);
}

void ThrudocHandler::remove (const string & tablename, const string & key)
{
    LOG4CXX_DEBUG (logger, "remove: tablename=" + tablename + ", key=" + key);
    this->backend->validate (tablename, &key, NULL);
    this->backend->remove (tablename, key);
}

void ThrudocHandler::scan (ScanResponse & _return, const string & tablename,
                           const string & seed, int32_t count)
{
    if (logger->isDebugEnabled())
    {
        char buf[256];
        sprintf (buf, "scan: tablename=%s, seed=%s, count=%d",
                 tablename.c_str (), seed.c_str (), count);
        LOG4CXX_DEBUG (logger, buf);
    }
    this->backend->validate (tablename, NULL, NULL);
    _return = this->backend->scan (tablename, seed, count);
}

void ThrudocHandler::admin (string & _return, const string & op, const string & data)
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

void ThrudocHandler::putList(vector<ThrudocException> & _return, 
                             const vector<Element> & elements)
{
    if (logger->isDebugEnabled())
    {
        char buf[128];
        sprintf (buf, "putList: elements.size=%d", (int)elements.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    _return = this->backend->putList (elements);
}

void ThrudocHandler::getList(vector<ListResponse> & _return, 
                             const vector<Element> & elements)
{
    if (logger->isDebugEnabled())
    {
        char buf[128];
        sprintf (buf, "putList: elements.size=%d", (int)elements.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    _return = this->backend->getList (elements);
}

void ThrudocHandler::removeList(vector<ThrudocException> & _return, 
                                const vector<Element> & elements)
{
    if (logger->isDebugEnabled())
    {
        char buf[128];
        sprintf (buf, "putList: elements.size=%d", (int)elements.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    _return = this->backend->removeList (elements);
}

void ThrudocHandler::putValueList(vector<ListResponse> & _return, 
                                  const vector<Element> & elements)
{
#if HAVE_LIBUUID
    if (logger->isDebugEnabled())
    {
        char buf[128];
        sprintf (buf, "putList: elements.size=%d", (int)elements.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    uuid_t uuid;
    char uuid_str[37];
    vector<Element> e = (vector<Element>)elements;
    vector<Element>::iterator i;
    for (i = e.begin (); i != e.end (); i++)
    {
        uuid_generate(uuid);
        uuid_unparse_lower(uuid, uuid_str);
        (*i).key = uuid_str;
    }
    vector<ThrudocException> exceptions = this->backend->putList (elements);
    for (size_t j = 0; j < exceptions.size(); j++)
    {
        ListResponse list_response;
        list_response.ex = exceptions[j];
        list_response.element = elements[j];
        _return.push_back (list_response);
    }
#else
    ThrudocException e;
    e.what = "putValue uuid generation not built";
    throw e;
#endif
}

