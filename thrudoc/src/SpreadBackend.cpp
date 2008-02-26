#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#if HAVE_LIBSPREAD

#include "SpreadBackend.h"

#if HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

// should be max expected key + max bucket + ~10. truncation will 
// occur otherwise
#define SPREAD_BACKEND_MAX_MESSAGE_SIZE 128

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

// private
LoggerPtr SpreadBackend::logger (Logger::getLogger ("SpreadBackend"));

SpreadBackend::SpreadBackend (shared_ptr<ThrudocBackend> backend,
                              const string & spread_name, 
                              const string & spread_private_name,
                              const string & spread_group) :
    spread (spread_name, spread_private_name)
{
    LOG4CXX_INFO (logger, "SpreadBackend: spread_name=" + spread_name + 
                  ", spread_private_name=" + spread_private_name + 
                  ", spread_group=" + spread_group);

    this->spread_group = spread_group;
    this->set_backend (backend);

    // TODO: joining isn't really necessary, you can send to a group without
    // being a member, but it's handy to be able to see who's on the line. will
    // this cause message to be stored up waiting for us to read them tho?
    // this->spread.join (this->spread_group);
}

string SpreadBackend::generate_uuid ()
{
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return string (uuid_str);
}

void SpreadBackend::put (const string & bucket, const string & key, 
                         const string & value)
{
    this->get_backend ()->put (bucket, key, value);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s put %s %s", 
              generate_uuid ().c_str (), bucket.c_str (), key.c_str ());
    this->spread.send (SAFE_MESS | SELF_DISCARD, this->spread_group, 1, 
                       msg, strlen (msg));
}

void SpreadBackend::remove (const string & bucket, const string & key )
{
    this->get_backend ()->remove (bucket, key);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s remove %s %s", 
              generate_uuid ().c_str (), bucket.c_str (), key.c_str ());
    this->spread.send (SAFE_MESS | SELF_DISCARD, this->spread_group, 1, 
                       msg, strlen (msg));
}

string SpreadBackend::admin (const string & op, const string & data)
{
    string ret = this->get_backend ()->admin (op, data);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s admin %s %s", 
              generate_uuid ().c_str (), op.c_str (), data.c_str ());
    this->spread.send (SAFE_MESS | SELF_DISCARD, this->spread_group, 1, 
                       msg, strlen (msg));
    return ret;
}

#endif /* HAVE_LIBSPREAD */
