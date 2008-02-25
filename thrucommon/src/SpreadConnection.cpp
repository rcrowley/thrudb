
#include "SpreadConnection.h"

using namespace log4cxx;
using namespace std;

string SP_error_to_string (int error);

LoggerPtr SpreadConnection::logger (Logger::getLogger ("SpreadConnection"));

SpreadConnection::SpreadConnection (const string & name,
                                    const string & private_name)
{
    char buf[1024];
    sprintf (buf, "SpreadConnection: name=%s, private_name=%s, group=%s",
             name.c_str (), private_name.c_str (), group.c_str ());
    LOG4CXX_INFO (logger, buf);

    this->name = name;
    this->private_name = private_name;
    this->group = group;

    // connection
    char private_group[MAX_GROUP_NAME];
    int ret = SP_connect (this->name.c_str (),
                          this->private_name.c_str (), 0, 1,
                          &this->mbox, private_group);
    if (ret < 0)
    {
        string error = SP_error_to_string (ret);
        LOG4CXX_ERROR (logger, error);
        // TODO: throw an exception here
        exit (-1);
    }

    // save our private group name
    this->private_group = private_group;
    LOG4CXX_INFO (logger, "SpreadConnection: private_group=" +
                  this->private_group);
}

SpreadConnection::~SpreadConnection ()
{
    LOG4CXX_INFO (logger, "~SpreadConnection");
    map<string, vector<string> >::iterator i;
    for (i = this->groups.begin ();
         i != this->groups.end ();
         i++)
    {
        SP_leave (this->mbox, (*i).first.c_str ());
    }
    SP_disconnect (this->mbox);
}

void SpreadConnection::join (const std::string & group)
{
    LOG4CXX_DEBUG (logger, "join: group=" + group);
    map<string, vector<string> >::iterator i;
    i = this->groups.find (group.c_str ());
    if (i != this->groups.end ())
    {
        LOG4CXX_DEBUG (logger, "    already a member");
    }
    else
    {
        int ret = SP_join (this->mbox, group.c_str ());
        if (ret < 0)
        {
            string error = SP_error_to_string (ret);
            LOG4CXX_ERROR (logger, error);
            // TODO: throw an exception
        }
        // this will create the groups key in the map, but we'll have to wait
        // for a membership message before we'll have the list of people
        // that's handled elsewhere, as are membership changes
        this->groups[group];
        LOG4CXX_DEBUG (logger, "    joined");
    }
}

void SpreadConnection::leave (const std::string & group)
{
    LOG4CXX_DEBUG (logger, "leave: group=" + group);
    map<string, vector<string> >::iterator i;
    i = this->groups.find (group.c_str ());
    if (i != this->groups.end ())
    {
        SP_leave (this->mbox, group.c_str ());
        // TODO: this will unsubscribe everyone...
        this->groups.erase (i);
        LOG4CXX_DEBUG (logger, "    left");
    }
}

void SpreadConnection::subscribe (const string & sender, const string & group,
                                  const int message_type,
                                  SubscriberCallbackInfo * callback)
{
    char buf[256];
    sprintf (buf, "subscribe: sender=%s, group=%s, message_type=%d", 
             sender.c_str (), group.c_str (), message_type);
    LOG4CXX_DEBUG (logger, buf);
    if (!group.empty ())
    {
        // make sure we're a member of the group in question
        this->join (group);
    }
    // and subscribe the caller up
    subscriptions[sender][group][message_type].push_back (callback);
}

// message will be copied in to a local buffer
void SpreadConnection::queue (const string & group, const int message_type,
                              const char * message, const int message_len)
{
    if (logger->isDebugEnabled ())
    {
        char buf[128];
        sprintf (buf, "queue: group=%s, message_type=%d", group.c_str (),
                 message_type);
        LOG4CXX_DEBUG (logger, buf);
    }
    QueuedMessage * queued_message =
        (QueuedMessage*)malloc (sizeof (QueuedMessage));
    queued_message->group = strndup (group.c_str (), group.length ());
    queued_message->message_type = message_type;
    queued_message->message = strndup (message, message_len);
    queued_message->message_len = message_len;
    this->pending_messages.push (queued_message);
}

void SpreadConnection::run (int count)
{
    if (logger->isDebugEnabled ())
    {
        char buf[64];
        sprintf (buf, "run: count=%d", count);
        LOG4CXX_DEBUG (logger, buf);
    }
    // send out any pending message
    this->drain_pending ();

    service service_type;
    char sender[MAX_GROUP_NAME];
    char max_groups = 2;
    int num_groups;
    // TODO: grow groups as necessary
    char groups[max_groups][MAX_GROUP_NAME];
    int16_t type;
    int endian_mismatch;
#define MAX_MESSAGE_SIZE 1024
    int buf_size = MAX_MESSAGE_SIZE;
    int buf_len;
    // TODO: grow buffer as necessary...
    char buf[buf_size];

    int i = 0;
    while (!count || i < count)
    {
        LOG4CXX_DEBUG (logger, "run:    receiving");
        buf_len = SP_receive (this->mbox, &service_type, sender,
                              max_groups, &num_groups, groups, &type,
                              &endian_mismatch, buf_size, buf);
        // TODO: tmp
        {
            char buf[128];
            sprintf (buf, "recv: buf_len=%d, sender=%s, group=%s, type=%d",
                     buf_len, sender, groups[0], type);
            LOG4CXX_DEBUG (logger, buf);
        }
        if (buf_len > 0)
        {
            // TODO: null terminate the message, can't hurt and we, so long as
            // it's not size - 1 in length...
            buf[buf_len] = '\0';
            LOG4CXX_DEBUG (logger, string ("receive: buf=") + buf);
            // TODO: dispatch
        }
        else
        {
            // TODO: error handling
            switch (buf_len)
            {
                case ILLEGAL_SESSION:
                    // ILLEGAL_SESSION - The mbox given to receive on was illegal.
                    //"replication: spread error ILLEGAL_SESSION, stopping";
                    break;
                case ILLEGAL_MESSAGE:
                    // ILLEGAL_MESSAGE - The message had an illegal structure, like a scatter not filled out correctly.
                    //"replication: spread error ILLEGAL_MESSAGE, ignoring";
                    break;
                case CONNECTION_CLOSED:
                    // CONNECTION_CLOSED - During  communication  to  receive  the  message  communication errors occured and the receive could not be completed.
                    //"replication: spread error CONNECTION_CLOSED";
                    // TODO: try and reconnect
                    break;
                case GROUPS_TOO_SHORT:
                    // GROUPS_TOO_SHORT - If the groups array is too short to hold  the  entire  list  of groups this message was sent to then this error is returned and the num_groups field will be set to the negative of the  number of groups needed.
                    //"replication: spread error GROUPS_TOO_SHORT, stopping";
                    break;
                case BUFFER_TOO_SHORT:
                    // BUFFER_TOO_SHORT - If  the  message body buffer mess is too short to hold the mes‐ sage being  received  then  this  error  is  returned  and  the endian_mismatch  field  is  set  to  the  negative value of the required buffer length.
                    //"replication: spread error BUFFER_TOO_SHORT, stopping";
                    break;
            }
        }
        // drain any pending messages
        this->drain_pending ();

        i++;
    }
    LOG4CXX_DEBUG (logger, "run:    done");
}

void SpreadConnection::make_callbacks
(vector<SubscriberCallbackInfo *> callbacks, const string & sender,
 const string & group, const int message_type, const char * message,
 const int message_len)
{
    if (logger->isDebugEnabled ())
    {
        char buf[256];
        sprintf (buf, "make_callbacks: callbacks.size=%d, sender=%s, group=%s, message_type=%d",
                 (int)callbacks.size (), sender.c_str (), group.c_str (), 
                 message_type);
        LOG4CXX_DEBUG (logger, buf);
    }
    bool ret;
    vector<SubscriberCallbackInfo *>::iterator i;
    for (i = callbacks.begin (); i != callbacks.end (); i++)
    {
        subscriber_callback call = (*i)->callback;
        void * data = (*i)->data;
        ret = (call)(this, sender, group, message_type, message, 
                     message_len, data);
        ret = ((*i)->callback)(this, sender, group, message_type, message, 
                               message_len, (*i)->data);
        if (!ret)
            callbacks.erase (i);
    }
}

void SpreadConnection::dispatch (const string & sender, const string & group,
                                 const int message_type, const char * message,
                                 const int message_len)
{
    if (logger->isDebugEnabled ())
    {
        char buf[128];
        sprintf (buf, "dispatch: sender=%s, group=%s, message_type=%d",
                 sender.c_str (), group.c_str (), message_type);
        LOG4CXX_DEBUG (logger, buf);
    }

    // if we haven't installed any callbacks, there's no reason to continue
    if (this->subscriptions.empty ())
        return;

    // TODO: recieve self flag
    if (this->private_group == sender && !this->receive_self)
        return;

    // holy fuck this is a mess, this could be used as a proof of how shitty
    // C++ is compared to perl where this is 8 lines of code :(

    map<string, map<string, map<int, 
        vector<SubscriberCallbackInfo *> > > >::iterator s = 
            this->subscriptions.find (sender);
    if (s != this->subscriptions.end ())
    {
        map<string, map<int, 
            vector<SubscriberCallbackInfo *> > >::iterator s_g =
                (*s).second.find (group);
        if (s_g != (*s).second.end ())
        {
            map<int, vector<SubscriberCallbackInfo *> >::iterator s_g_t =
                (*s_g).second.find (message_type);
            if (s_g_t != (*s_g).second.end ())
            {
                // we have a sender, group, type match
                this->make_callbacks ((*s_g_t).second, sender, group,
                                      message_type, message, message_len);
            }
            map<int, vector<SubscriberCallbackInfo *> >::iterator s_g_et =
                (*s_g).second.find (-1);
            if (s_g_et != (*s_g).second.end ())
            {
                // we have a sender, group, empty type match
                this->make_callbacks ((*s_g_et).second, sender, group,
                                      message_type, message, message_len);
            }
        }
        map<string, map<int, 
            vector<SubscriberCallbackInfo *> > >::iterator s_eg =
                (*s).second.find ("");
        if (s_eg != (*s).second.end ())
        {
            map<int, vector<SubscriberCallbackInfo *> >::iterator s_eg_t =
                (*s_eg).second.find (message_type);
            if (s_eg_t != (*s_eg).second.end ())
            {
                // we have a sender, empty group, type match
                this->make_callbacks ((*s_eg_t).second, sender, group,
                                      message_type, message, message_len);
            }
            map<int, vector<SubscriberCallbackInfo *> >::iterator s_eg_et =
                (*s_eg).second.find (-1);
            if (s_eg_et != (*s_eg).second.end ())
            {
                // we have a sender, empty group, empty type match
                this->make_callbacks ((*s_eg_et).second, sender, group,
                                      message_type, message, message_len);
            }
        }
    }
    map<string, map<string, map<int, 
        vector<SubscriberCallbackInfo *> > > >::iterator
            es = this->subscriptions.find ("");
    if (es != this->subscriptions.end ())
    {
        map<string, map<int, 
            vector<SubscriberCallbackInfo *> > >::iterator es_g =
                (*es).second.find (group);
        if (es_g != (*es).second.end ())
        {
            map<int, vector<SubscriberCallbackInfo *> >::iterator es_g_t =
                (*es_g).second.find (message_type);
            if (es_g_t != (*es_g).second.end ())
            {
                // we have a empty sender, group, type match
                this->make_callbacks ((*es_g_t).second, sender, group,
                                      message_type, message, message_len);
            }
            map<int, vector<SubscriberCallbackInfo *> >::iterator es_g_et =
                (*es_g).second.find (-1);
            if (es_g_et != (*es_g).second.end ())
            {
                // we have a empty sender, group, empty type match
                this->make_callbacks ((*es_g_et).second, sender, group,
                                      message_type, message, message_len);
            }
        }
        map<string, map<int, 
            vector<SubscriberCallbackInfo *> > >::iterator es_eg =
            (*es).second.find ("");
        if (es_eg != (*es).second.end ())
        {
            map<int, vector<SubscriberCallbackInfo *> >::iterator es_eg_t =
                (*es_eg).second.find (message_type);
            if (es_eg_t != (*es_eg).second.end ())
            {
                // we have a empty sender, empty group, type match
                this->make_callbacks ((*es_eg_t).second, sender, group,
                                      message_type, message, message_len);
            }
            map<int, vector<SubscriberCallbackInfo *> >::iterator es_eg_et =
                (*es_eg).second.find (-1);
            if (es_eg_et != (*es_eg).second.end ())
            {
                // we have a empty sender, empty group, empty type match
                this->make_callbacks ((*es_eg_et).second, sender, group,
                                      message_type, message, message_len);
            }
        }
    }
}

void SpreadConnection::drain_pending ()
{
    if (logger->isDebugEnabled ())
    {
        char buf[64];
        sprintf (buf, "drain_pending: pending_messages.size=%d", 
                 (int)this->pending_messages.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    int ret;
    QueuedMessage * qm;
    while (!this->pending_messages.empty ())
    {
        LOG4CXX_DEBUG (logger, "drain_pending:    draining");
        qm = this->pending_messages.front ();
        this->pending_messages.pop ();
        ret = SP_multicast (this->mbox, SAFE_MESS | SELF_DISCARD,
                            qm->group, qm->message_type,
                            qm->message_len, qm->message);
        // TODO: error handling
        free (qm->group);
        free (qm->message);
        free (qm);
    }
    LOG4CXX_DEBUG (logger, "drain_pending: done");
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
