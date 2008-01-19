
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

void ThrudocHandler::getBuckets (vector<string> & _return)
{
    LOG4CXX_DEBUG (logger, "getBuckets: ");
    _return = this->backend->getBuckets ();
}

void ThrudocHandler::put (const string & bucket, const string & key,
                          const string & value)
{
    LOG4CXX_DEBUG (logger, "put: bucket=" + bucket + ", key=" + key +
                   ", value=" + value);
    this->backend->validate (bucket, &key, &value);
    this->backend->put (bucket, key, value);
}

void ThrudocHandler::putValue (string & _return, const string & bucket,
                               const string & value)
{
#if HAVE_LIBUUID
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    _return = string (uuid_str);
    LOG4CXX_DEBUG (logger, "putValue: bucket=" + bucket + ", value=" +
                   value + ", uuid=" + _return);
    this->backend->put (bucket, _return, value);
#else
    ThrudocException e;
    e.what = "putValue uuid generation not built";
    throw e;
#endif
}

void ThrudocHandler::get (string & _return, const string & bucket,
                          const string & key)
{
    LOG4CXX_DEBUG (logger, "get: bucket=" + bucket + ", key=" + key);
    this->backend->validate (bucket, &key, NULL);
    _return = this->backend->get (bucket, key);
}

void ThrudocHandler::remove (const string & bucket, const string & key)
{
    LOG4CXX_DEBUG (logger, "remove: bucket=" + bucket + ", key=" + key);
    this->backend->validate (bucket, &key, NULL);
    this->backend->remove (bucket, key);
}

void ThrudocHandler::scan (ScanResponse & _return, const string & bucket,
                           const string & seed, int32_t count)
{
    if (logger->isDebugEnabled())
    {
        char buf[256];
        sprintf (buf, "scan: bucket=%s, seed=%s, count=%d",
                 bucket.c_str (), seed.c_str (), count);
        LOG4CXX_DEBUG (logger, buf);
    }
    this->backend->validate (bucket, NULL, NULL);
    _return = this->backend->scan (bucket, seed, count);
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
        sprintf (buf, "getList: elements.size=%d", (int)elements.size ());
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
        sprintf (buf, "removeList: elements.size=%d", (int)elements.size ());
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
        sprintf (buf, "putValueList: elements.size=%d", (int)elements.size ());
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

