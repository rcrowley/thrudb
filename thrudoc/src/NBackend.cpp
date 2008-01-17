#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "NBackend.h"

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

// private
LoggerPtr NBackend::logger (Logger::getLogger ("NBackend"));

NBackend::NBackend (vector<shared_ptr<ThrudocBackend> > backends)
{
    LOG4CXX_INFO (logger, "NBackend");
    this->backends = backends;
}

NBackend::~NBackend ()
{
}

vector<string> NBackend::getBuckets ()
{
    vector<string> buckets;
    vector<shared_ptr<ThrudocBackend> >::iterator i;
    // TODO: might be nice to do a union of all the backends...
    for (i = backends.begin (); i != backends.end (); i++)
    {
        vector<string> tns = (*i)->getBuckets ();
        buckets.insert (buckets.end (), tns.begin (), tns.end ());
    }
    return buckets;
}

string NBackend::get (const string & bucket, const string & key )
{
    return (*backends.begin ())->get (bucket, key);
}

void NBackend::put (const string & bucket, const string & key, 
                    const string & value)
{
    vector<shared_ptr<ThrudocBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->put (bucket, key, value);
    }
}

void NBackend::remove (const string & bucket, const string & key )
{
    vector<shared_ptr<ThrudocBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->remove (bucket, key);
    }
}

ScanResponse NBackend::scan (const string & bucket,
                             const string & seed, int32_t count)
{
    return (*backends.begin ())->scan (bucket, seed, count);
}

string NBackend::admin (const string & op, const string & data)
{
    string ret;
    vector<shared_ptr<ThrudocBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        ret += (*i)->admin (op, data) + ";";
    }
    return ret;
}

void NBackend::validate (const string & bucket, const string * key, 
                         const string * value)
{
    vector<shared_ptr<ThrudocBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->validate (bucket, key, value);
    }
}
