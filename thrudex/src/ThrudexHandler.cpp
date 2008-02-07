#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H


#include "ThrudexHandler.h"

using namespace std;
using namespace boost;
using namespace thrudex;
using namespace log4cxx;

LoggerPtr ThrudexHandler::logger (Logger::getLogger ("ThrudexHandler"));

ThrudexHandler::ThrudexHandler(shared_ptr<ThrudexBackend> backend)
{
    this->backend = backend;
}

void ThrudexHandler::ping()
{
    //just return
}

void ThrudexHandler::getIndices(vector<string> &_return)
{
    _return = backend->getIndices();
}

void ThrudexHandler::put(const Document &d)
{
    backend->put(d);
}

void ThrudexHandler::remove(const Element &e)
{
    backend->remove(e);
}

void ThrudexHandler::search(SearchResponse &_return, const SearchQuery &s)
{
    backend->search(s, _return);
}

void ThrudexHandler::putList(vector<ThrudexException> &_return, const vector<Document> &documents)
{
    _return = backend->putList(documents);
}


void ThrudexHandler::removeList(vector<ThrudexException> &_return, const vector<Element> &elements)
{
    _return = backend->removeList(elements);
}

void ThrudexHandler::searchList(vector<SearchResponse> &_return, const vector<SearchQuery> &q)
{
    _return = backend->searchList(q);
}

void ThrudexHandler::admin(string &_return, const string &op, const string &data)
{
    _return = backend->admin(op,data);
}

