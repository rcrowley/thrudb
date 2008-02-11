#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "SpreadReplicationBackend.h"

#if HAVE_LIBSPREAD && HAVE_LIBUUID

// should be max expected key + value + uuid size + ~4. truncation occur
// otherwise
#define MAX_BUCKET_SIZE 64
#define MAX_KEY_SIZE 64
#define MAX_VALUE_SIZE 128
// TODO: correct value...
#define UUID_LEN 37
#define MESSAGE_OVERHEAD 4
#define SPREAD_BACKEND_MAX_MESSAGE_SIZE MAX_BUCKET_SIZE + MAX_KEY_SIZE + MAX_VALUE_SIZE + UUID_LEN + MESSAGE_OVERHEAD

#define ORIG_MESSAGE_TYPE 1
#define REPLAY_MESSAGE_TYPE 101

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

string SP_error_to_string (int error);

// private
LoggerPtr SpreadReplicationBackend::logger (Logger::getLogger ("SpreadReplicationBackend"));

SpreadReplicationBackend::SpreadReplicationBackend (shared_ptr<ThrudocBackend> backend,
                                                    const string & spread_name, 
                                                    const string & spread_private_name,
                                                    const string & spread_group) 
{
    LOG4CXX_INFO (logger, string ("SpreadReplicationBackend: spread_name=") + 
                  spread_name + ", spread_private_name=" +
                  spread_private_name + ", spread_group=" + spread_group);

    this->set_backend (backend);
    this->spread_name = spread_name;
    this->spread_private_name = spread_private_name;
    this->spread_group = spread_group;

    char private_group[MAX_GROUP_NAME];
    int ret = SP_connect (this->spread_name.c_str (), 
                          this->spread_private_name.c_str (), 0, 1,
                          &this->spread_mailbox, private_group);
    if (ret < 0)
    {
        string error = SP_error_to_string (ret);
        LOG4CXX_ERROR (logger, error);
        ThrudocException e;
        e.what = error;
        throw e;
    }

    this->spread_private_group = string (private_group);
    LOG4CXX_INFO (logger, "SpreadBackend: private_group=" + 
                  this->spread_private_group);

    ret = SP_join (this->spread_mailbox, this->spread_group.c_str ());
    if (ret < 0)
    {
        string error = SP_error_to_string (ret);
        LOG4CXX_ERROR (logger, error);
        ThrudocException e;
        e.what = error;
        throw e;
    }

    // create the listener thread
    if (pthread_create(&listener_thread, NULL, start_listener_thread, 
                       (void *)this) != 0) 
    {
        char error[] = "SpreadReplicationBackend: start_listener_thread failed\n";
        LOG4CXX_ERROR (logger, error);
        ThrudocException e;
        e.what = error;
        throw e;
    }
    listener_thread_go = true;
}

SpreadReplicationBackend::~SpreadReplicationBackend ()
{
    this->listener_thread_go = false;
    // TODO: make sure there's a thread to join
    pthread_join(listener_thread, NULL);
    SP_disconnect (this->spread_mailbox);
}

// TODO: these are going to return success, etc. without us really knowing if
// they're going to, that kinda sucks. best we can do is validate tho it's also
// kinda funky that putValue will return an id that may never come in to
// existence

// TODO: implement circuit breaker pattern around spread...

// TODO: implement a get that will (optionally) reflect things that we've sent
// out via spread, but haven't heard back about yet.

void SpreadReplicationBackend::put (const string & bucket, const string & key, 
                                    const string & value)
{
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s p %s %s %s", 
              generate_uuid ().c_str (), bucket.c_str (), key.c_str (),
              value.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS, this->spread_group.c_str (),
                  ORIG_MESSAGE_TYPE, strlen (msg), msg);
}

void SpreadReplicationBackend::remove (const string & bucket, 
                                       const string & key )
{
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s r %s %s", 
              generate_uuid ().c_str (), bucket.c_str (), key.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS, this->spread_group.c_str (),
                  ORIG_MESSAGE_TYPE, strlen (msg), msg);
}

string SpreadReplicationBackend::admin (const string & op, const string & data)
{
    char msg[SPREAD_BACKEND_MAX_MESSAGE_SIZE];
    snprintf (msg, SPREAD_BACKEND_MAX_MESSAGE_SIZE, "%s a %s %s", 
              generate_uuid ().c_str (), op.c_str (), data.c_str ());
    SP_multicast (this->spread_mailbox, SAFE_MESS, this->spread_group.c_str (),
                  ORIG_MESSAGE_TYPE, strlen (msg), msg);
    // for the most part admin operations are idempotent, so we'll go ahead
    // and run this one locally to get an idea of what will happen via spread
    // and so we can return something "correct" to the caller ... 
    // TODO: is this the best approach?
    return this->get_backend ()->admin (op, data); 
}

void SpreadReplicationBackend::validate (const std::string & bucket,
                                         const std::string * key,
                                         const std::string * value)
{
    this->get_backend ()->validate (bucket, key, value);
    if (bucket.length () > MAX_BUCKET_SIZE)
    {
        ThrudocException e;
        e.what = "bucket too large";
        throw e;
    }
    if (key != NULL && key->length () > MAX_KEY_SIZE)
    {
        ThrudocException e;
        e.what = "key too large";
        throw e;
    }
    if (value != NULL && value->length () > MAX_VALUE_SIZE)
    {
        ThrudocException e;
        e.what = "value too large";
        throw e;
    }
    // so long as the bucket, key, and value have valid sizes our msg len
    // should be ok
}

string SpreadReplicationBackend::generate_uuid ()
{
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return string (uuid_str);
}

void * SpreadReplicationBackend::start_listener_thread (void * ptr)
{
    LOG4CXX_INFO (logger, "start_listener_thread: ");
    (((SpreadReplicationBackend*)ptr)->listener_thread_run ());
    return NULL;
}

int parse_orig_message (char * mess, char * uuid, char * op, 
                        char * bucket, char * key, char * value)
{
    return sscanf (mess, "%s %c %s %s %s", uuid, op, bucket, key, value);
}

int parse_replay_message (char * mess, char * orig_sender, 
                          int16_t * orig_message_type, char * uuid, char * op, 
                          char * bucket, char * key, char * value)
{
    return sscanf (mess, "%s;%hu;%s %c %s %s %s", orig_sender, 
                   orig_message_type, uuid, op, bucket, key, value);
}

void SpreadReplicationBackend::listener_thread_run ()
{
    service service_type;
    char sender[MAX_GROUP_NAME];
    char max_groups = 5;
    int num_groups;
    char groups[max_groups][MAX_GROUP_NAME];
    int16_t mess_type;
    int endian_mismatch;
    int max_mess_len = SPREAD_BACKEND_MAX_MESSAGE_SIZE;
    char mess[SPREAD_BACKEND_MAX_MESSAGE_SIZE];

    char last_uuid[UUID_LEN + 1];
    last_uuid[UUID_LEN] = '\0';

    int mess_len;
    while (this->listener_thread_go)
    {
        // TODO: poll so that we're non-blocking

        // TODO: is spread thread safe, enough, can we multi-thread write,
        // and single thread read all at the same time?
        mess_len = SP_receive (this->spread_mailbox, &service_type, sender,
                               max_groups, &num_groups, groups, &mess_type, 
                               &endian_mismatch, max_mess_len, mess);
        if (mess_len > 0)
        {
            mess[mess_len] = '\0';

            char uuid[UUID_LEN];
            char op = 0;
            char bucket[MAX_BUCKET_SIZE];
            char key[MAX_KEY_SIZE];
            char value[MAX_VALUE_SIZE];
            if (mess_type == ORIG_MESSAGE_TYPE)
            {
                parse_orig_message (mess, uuid, &op, bucket, key, value);
            }
            else if (mess_type == REPLAY_MESSAGE_TYPE)
            {
                char orig_sender[MAX_GROUP_NAME];
                int16_t orig_mess_type;
                parse_replay_message (mess, orig_sender, &orig_mess_type, uuid, 
                                      &op, bucket, key, value);
            }

            try
            {
                switch (op)
                {
                    case 'p':
                        LOG4CXX_DEBUG (logger, string ("replication: put: bucket=") + 
                                       bucket + ", key=" + key + ", value=" + 
                                       value);
                        this->get_backend ()->put (bucket, key, value);
                        break;
                    case 'r':
                        LOG4CXX_DEBUG (logger, string ("replication remove: bucket=") + 
                                       bucket + ", key=" + key);
                        this->get_backend ()->remove (bucket, key);
                        break;
                    case 'a':
                        LOG4CXX_DEBUG (logger, string ("replication admin: op=") + 
                                       bucket + ", data=" + key);
                        this->get_backend ()->admin (bucket, key);
                        break;
                    case 0:
                    default:
                        LOG4CXX_DEBUG (logger, string ("replication admin: unknown op"));
                        break;
                }
                // copy over the last uuid we've completed
                memcpy (last_uuid, uuid, UUID_LEN);
                // flush_last every so often
            }
            catch (ThrudocException & e)
            {
                // we had an exception, so we'll stop replication now
                this->listener_thread_go = false;
                LOG4CXX_ERROR (logger, string ("replication failure: uuid=") +
                               uuid + ", what=" + e.what);
                LOG4CXX_ERROR (logger, string ("replication failure: last successful uuid=") + 
                               last_uuid);
                // force flush_last now, note it's the old last
            }
        }
        else
        {
            // spread error
            string msg;
            switch (mess_type)
            {
                case ILLEGAL_SESSION:
                    // ILLEGAL_SESSION - The mbox given to receive on was illegal.
                    msg = "replication: spread error ILLEGAL_SESSION, stopping";
                    this->listener_thread_go = false;
                    break;
                case ILLEGAL_MESSAGE:
                    // ILLEGAL_MESSAGE - The message had an illegal structure, like a scatter not filled out correctly.
                    msg = "replication: spread error ILLEGAL_MESSAGE, ignoring";
                    break;
                case CONNECTION_CLOSED:
                    // CONNECTION_CLOSED - During  communication  to  receive  the  message  communication errors occured and the receive could not be completed.  
                    msg = "replication: spread error CONNECTION_CLOSED";
                    // TODO: try and reconnect
                    break;
                case GROUPS_TOO_SHORT:
                    // GROUPS_TOO_SHORT - If the groups array is too short to hold  the  entire  list  of groups this message was sent to then this error is returned and the num_groups field will be set to the negative of the  number of groups needed.
                    msg = "replication: spread error GROUPS_TOO_SHORT, stopping";
                    this->listener_thread_go = false;
                    break;
                case BUFFER_TOO_SHORT:
                    // BUFFER_TOO_SHORT - If  the  message body buffer mess is too short to hold the mes‐ sage being  received  then  this  error  is  returned  and  the endian_mismatch  field  is  set  to  the  negative value of the required buffer length.
                    msg = "replication: spread error BUFFER_TOO_SHORT, stopping";
                    this->listener_thread_go = false;
                    break;
            }
            LOG4CXX_ERROR (logger, msg);
        }
    }
}

#endif /* HAVE_LIBSPREAD && HAVE_LIBUUID */
