#ifdef HAVE_CONFIG_H
#include "diststore_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "NBackend.h"

#if HAVE_LIBMEMCACHED

// private
LoggerPtr NBackend::logger (Logger::getLogger ("NBackend"));

NBackend::NBackend (vector<shared_ptr<DistStoreBackend> > backends)
{
    LOG4CXX_INFO (logger, "NBackend");
    this->backends = backends;
}

NBackend::~NBackend ()
{
}

vector<string> NBackend::getTablenames ()
{
    vector<string> tablenames;
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    // TODO: might be nice to do a union of all the backends...
    for (i = backends.begin (); i != backends.end (); i++)
    {
        vector<string> tns = (*i)->getTablenames ();
        tablenames.insert (tablenames.end (), tns.begin (), tns.end ());
    }
    return tablenames;
}

string NBackend::get (const string & tablename, const string & key )
{
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    return (*backends.begin ())->get (tablename, key);
}

void NBackend::put (const string & tablename, const string & key, 
                    const string & value)
{
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->put (tablename, key, value);
    }
}

void NBackend::remove (const string & tablename, const string & key )
{
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->remove (tablename, key);
    }
}

ScanResponse NBackend::scan (const string & tablename,
                             const string & seed, int32_t count)
{
    return (*backends.begin ())->scan (tablename, seed, count);
}

string NBackend::admin (const string & op, const string & data)
{
    string ret;
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        ret += (*i)->admin (op, data) + ";";
    }
    return ret;
}

void NBackend::validate (const string & tablename, const string * key, 
                         const string * value)
{
    vector<shared_ptr<DistStoreBackend> >::iterator i;
    for (i = backends.begin (); i != backends.end (); i++)
    {
        (*i)->validate (tablename, key, value);
    }
}

#endif
