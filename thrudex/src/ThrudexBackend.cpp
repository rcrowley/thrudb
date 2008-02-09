#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "ThrudexBackend.h"

using namespace std;
using namespace boost;
using namespace thrudex;
using namespace log4cxx;

LoggerPtr ThrudexBackend::logger (Logger::getLogger ("ThrudexBackend"));


ThrudexBackend::ThrudexBackend()
{

}

ThrudexBackend::~ThrudexBackend()
{

}

vector<ThrudexException> ThrudexBackend::putList(const vector<Document> &documents)
{

    vector<ThrudexException> exceptions;
    ThrudexException none;

    vector<Document>::const_iterator it;

    for (it = documents.begin (); it != documents.end (); ++it)
    {
        try
        {
            this->put( (*it) );
            exceptions.push_back (none);
        }
        catch (ThrudexException e)
        {
            exceptions.push_back (e);
        }
    }

    return exceptions;
}

vector<ThrudexException> ThrudexBackend::removeList(const vector<Element> &elements)
{
    vector<ThrudexException> exceptions;
    ThrudexException none;

    vector<Element>::const_iterator it;

    for (it = elements.begin (); it != elements.end (); ++it)
    {
        try
        {
            this->remove( (*it) );
            exceptions.push_back (none);
        }
        catch (ThrudexException e)
        {
            exceptions.push_back (e);
        }
    }

    return exceptions;

}

vector<SearchResponse> ThrudexBackend::searchList(const vector<SearchQuery> &queries)
{

    vector<SearchResponse> responses;


    vector<SearchQuery>::const_iterator it;

    for(it = queries.begin (); it != queries.end (); ++it)
    {
        try
        {
            SearchResponse r;
            this->search( *it, r );
            responses.push_back (r);
        }
        catch (ThrudexException e)
        {
            SearchResponse r;

            r.ex = e;

            responses.push_back(r);
        }
    }

    return responses;
}

string ThrudexBackend::admin (const string & op, const string & data)
{
    if (op == "echo")
    {
        return data;
    }
    else if (op == "exit")
    {
        exit (0);
    }
    else if (op == "put_log_position")
    {
        return "done";
    }
    else if (op == "get_log_position")
    {
        return "";
    }
        
    return "";
}
