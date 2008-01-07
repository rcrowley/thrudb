#ifdef HAVE_CONFIG_H
#include "diststore_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "SpreadBackend.h"

#if HAVE_LIBSPREAD

// should be max expected key + max tablename + ~10. truncation will 
// occur otherwise
#define SPREAD_BACKEND_MAX_MESSAGE_SIZE 128

// private
LoggerPtr SpreadBackend::logger (Logger::getLogger ("SpreadBackend"));

SpreadBackend::SpreadBackend (const string & spread_name, 
                              const string & spread_private_name,
                              const string & spread_group,
                              shared_ptr<DistStoreBackend> backend)
{
    LOG4CXX_INFO (logger, "SpreadBackend: spread_name=" + spread_name + 
                  ", spread_private_name=" + spread_private_name + 
                  ", spread_group=" + spread_group);

    this->spread_name = spread_name;
    this->spread_private_name = spread_private_name;
    this->spread_group = spread_group;
    this->backend = backend;

    char private_group[MAX_GROUP_NAME];
    int ret = SP_connect (this->spread_name.c_str (), 
                          this->spread_private_name.c_str (), 0, 1,
                          &this->spread_mailbox, private_group);
    // TODO: this is annoying, better error handing, print to log etc.
    if( ret < 0 )
    {
        SP_error( ret );
        exit(0);
    }

    this->spread_private_group = string (private_group);
    LOG4CXX_INFO (logger, "SpreadBackend: private_group=" + 
                  this->spread_private_group);

    ret = SP_join (this->spread_mailbox, this->spread_group.c_str ());
    // TODO: this is annoying, better error handing, print to log etc.
    if( ret < 0 )
    {
        SP_error( ret );
        exit(0);
    }
}

SpreadBackend::~SpreadBackend ()
{
    SP_disconnect (spread_mailbox);
}

vector<string> SpreadBackend::getTablenames ()
{
    return this->backend->getTablenames ();
}

string SpreadBackend::get (const string & tablename, const string & key )
{
    return this->backend->get (tablename, key);
}

void SpreadBackend::put (const string & tablename, const string & key, 
                         const string & value)
{
    this->backend->put (tablename, key, value);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "put %s %s", 
              tablename.c_str (), key.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS | SELF_DISCARD, 
                  this->spread_group.c_str (), 0, strlen (msg), msg);
}

void SpreadBackend::remove (const string & tablename, const string & key )
{
    this->backend->remove (tablename, key);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "remove %s %s", 
              tablename.c_str (), key.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS | SELF_DISCARD, 
                  this->spread_group.c_str (), 0, strlen (msg), msg);
}

ScanResponse SpreadBackend::scan (const string & tablename,
                                  const string & seed, int32_t count)
{
    return this->backend->scan (tablename, seed, count);
}

string SpreadBackend::admin (const string & op, const string & data)
{
    return this->backend->admin (op, data);
}

void SpreadBackend::validate (const string * tablename, const string * key, 
                              const string * value)
{
    this->backend->validate (tablename, key, value);
}

#endif /* HAVE_LIBSPREAD */
