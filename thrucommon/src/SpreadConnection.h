/**
 *
 **/

#ifndef _SPREAD_CONNECTION_H_
#define _SPREAD_CONNECTION_H_

#include <log4cxx/logger.h>
#include <map>
#include <sp.h>
#include <string>
#include <queue>
#include <vector>

// only so we can pass this in to callbacks
class SpreadConnection;
/* callbacks return true to stay installed, false to be removed */
typedef bool (*subscriber_callback) (SpreadConnection * spread_connection,
                                     const std::string & sender,
                                     const std::string & group,
                                     const int message_type,
                                     const char * message,
                                     const int message_len,
                                     void * data);
typedef struct SubscriberCallbackInfo SubscriberCallbackInfo;
struct SubscriberCallbackInfo
{
    subscriber_callback callback;
    void * data;
};

struct QueuedMessage
{
    char * group;
    int message_type;
    char * message;
    int message_len;
};

class SpreadConnection
{
    public:
        SpreadConnection (const std::string & name,
                          const std::string & private_name);
        ~SpreadConnection ();

        void subscribe (const std::string & sender, const std::string & group,
                        const int message_type,
                        SubscriberCallbackInfo * callback);
        void queue (const std::string & group, const int message_type,
                    const char * message, const int message_len);
        void run (int count);

        void set_receive_self (bool receive_self)
        {
            this->receive_self = receive_self;
        }

        std::string get_private_group ()
        {
            return this->private_group;
        }

    private:
        static log4cxx::LoggerPtr logger;

        std::string name;
        std::string private_name;
        std::string group;
        std::string private_group;
        bool receive_self;
        mailbox mbox;
        std::map<std::string, std::vector<std::string> > groups;
        std::map<std::string, std::map<std::string, std::map<int,
            std::vector<SubscriberCallbackInfo *> > > > subscriptions;
        std::queue<QueuedMessage *> pending_messages;

        void join (const std::string & group);
        void leave (const std::string & group);
        void dispatch (const std::string & sender, const std::string & group,
                       const int message_type, const char * message,
                       const int message_len);
        void make_callbacks (std::vector<SubscriberCallbackInfo *> callbacks,
                             const std::string & sender,
                             const std::string & group,
                             const int message_type, const char * message,
                             const int message_len);
        void drain_pending ();
};

#endif /* _SPREAD_CONNECTION_H_ */
