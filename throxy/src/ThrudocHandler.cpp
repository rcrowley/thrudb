
#ifdef HAVE_CONFIG_H
#include "throxy_config.h"
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

ThrudocHandler::ThrudocHandler ()
{


}

void ThrudocHandler::getBuckets (vector<string> &_return)
{


}

void ThrudocHandler::put (const string &bucket, const string &key,
                          const string &value)
{


}

void ThrudocHandler::putValue (string &_return, const string &bucket,
                               const string &value)
{


}

void ThrudocHandler::get (string &_return, const string &bucket,
                          const string &key)
{


}

void ThrudocHandler::remove (const string &bucket, const string &key)
{


}

void ThrudocHandler::scan (ScanResponse &_return, const string &bucket,
                           const string &seed, int32_t count)
{

}

void ThrudocHandler::admin (string &_return, const string &op, const string &data)
{

}

void ThrudocHandler::putList(vector<ThrudocException> &_return,
                             const vector<Element> &elements)
{

}

void ThrudocHandler::getList(vector<ListResponse> &_return,
                             const vector<Element> &elements)
{

}

void ThrudocHandler::removeList(vector<ThrudocException> &_return,
                                const vector<Element> &elements)
{

}

void ThrudocHandler::putValueList(vector<ListResponse> &_return,
                                  const vector<Element> &elements)
{

}

