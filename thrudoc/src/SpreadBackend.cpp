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

string SP_error_to_string (int error);

SpreadBackend::SpreadBackend (shared_ptr<ThrudocBackend> backend,
                              const string & spread_name, 
                              const string & spread_private_name,
                              const string & spread_group)
{
    LOG4CXX_INFO (logger, "SpreadBackend: spread_name=" + spread_name + 
                  ", spread_private_name=" + spread_private_name + 
                  ", spread_group=" + spread_group);

    this->spread_name = spread_name;
    this->spread_private_name = spread_private_name;
    this->spread_group = spread_group;
    this->set_backend (backend);

    char private_group[MAX_GROUP_NAME];
    int ret = SP_connect (this->spread_name.c_str (), 
                          this->spread_private_name.c_str (), 0, 1,
                          &this->spread_mailbox, private_group);
    if (ret < 0)
    {
        string error = SP_error_to_string (ret);
        LOG4CXX_ERROR (logger, error);
        fprintf (stderr, error.c_str ());
        exit (1);
    }

    this->spread_private_group = string (private_group);
    LOG4CXX_INFO (logger, "SpreadBackend: private_group=" + 
                  this->spread_private_group);

    ret = SP_join (this->spread_mailbox, this->spread_group.c_str ());
    if (ret < 0)
    {
        string error = SP_error_to_string (ret);
        LOG4CXX_ERROR (logger, error);
        fprintf (stderr, error.c_str ());
        exit (1);
    }
}

SpreadBackend::~SpreadBackend ()
{
    SP_disconnect (spread_mailbox);
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
    SP_multicast (this->spread_mailbox, SAFE_MESS | SELF_DISCARD, 
                  this->spread_group.c_str (), 1, strlen (msg), msg);
}

void SpreadBackend::remove (const string & bucket, const string & key )
{
    this->get_backend ()->remove (bucket, key);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s remove %s %s", 
              generate_uuid ().c_str (), bucket.c_str (), key.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS | SELF_DISCARD, 
                  this->spread_group.c_str (), 1, strlen (msg), msg);
}

string SpreadBackend::admin (const string & op, const string & data)
{
    string ret = this->get_backend ()->admin (op, data);
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s admin %s %s", 
              generate_uuid ().c_str (), op.c_str (), data.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS | SELF_DISCARD, 
                  this->spread_group.c_str (), 1, strlen (msg), msg);
    return ret;
}

// copied from sp.c, redic that it's a function that aborts in a library rather
// than returning the error string to you to do something with, like log it
// maybe.
string SP_error_to_string (int error)
{
    // convert int to string
    char buf[10];
    sprintf (buf, "%d", error);
    string error_str (buf);
    // then wrap it in the error
    switch (error)
    {
        case ILLEGAL_SPREAD:
            return "SP_error: (" + error_str + ") Illegal spread was provided";
        case COULD_NOT_CONNECT:
            return "SP_error: (" + error_str + ") Could not connect. Is Spread running?";
        case REJECT_QUOTA:
            return "SP_error: (" + error_str + ") Connection rejected, to many users";
        case REJECT_NO_NAME:
            return "SP_error: (" + error_str + ") Connection rejected, no name was supplied";
        case REJECT_ILLEGAL_NAME:
            return "SP_error: (" + error_str + ") Connection rejected, illegal name";
        case REJECT_NOT_UNIQUE:
            return "SP_error: (" + error_str + ") Connection rejected, name not unique";
        case REJECT_VERSION:
            return "SP_error: (" + error_str + ") Connection rejected, library does not fit daemon";
        case CONNECTION_CLOSED:
            return "SP_error: (" + error_str + ") Connection closed by spread";
        case REJECT_AUTH:
            return "SP_error: (" + error_str + ") Connection rejected, authentication failed";
        case ILLEGAL_SESSION:
            return "SP_error: (" + error_str + ") Illegal session was supplied";
        case ILLEGAL_SERVICE:
            return "SP_error: (" + error_str + ") Illegal service request";
        case ILLEGAL_MESSAGE:
            return "SP_error: (" + error_str + ") Illegal message";
        case ILLEGAL_GROUP:
            return "SP_error: (" + error_str + ") Illegal group";
        case BUFFER_TOO_SHORT:
            return "SP_error: (" + error_str + ") The supplied buffer was too short";
        case GROUPS_TOO_SHORT:
            return "SP_error: (" + error_str + ") The supplied groups list was too short";
        case MESSAGE_TOO_LONG:
            return "SP_error: (" + error_str + ") The message body + group names was too large to fit in a message";
        case NET_ERROR_ON_SESSION:
            return "SP_error: (" + error_str + ") The network socket experienced an error. This Spread mailbox will no longer work until the connection is disconnected and then reconnected";
        default:
            return "SP_error: (" + error_str + ") unrecognized error";
    }
}

#endif /* HAVE_LIBSPREAD */
