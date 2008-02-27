/**
 *
 **/

#ifndef _REPLICIATION_BACKEND_H_
#define _REPLICIATION_BACKEND_H_

#if HAVE_LIBSPREAD && HAVE_LIBUUID

#include <EventLog_types.h>
#include <log4cxx/logger.h>
#include <queue>
#include <Spread.h>
#include <string>
#include <thrift/concurrency/Mutex.h>
#include <uuid/uuid.h>

#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

class ReplicationWait;

/* NOTE: this is not exactly a normal passthrough backend even tho it kinda
 * looks like one. we use the passthrough, but there's spread in between
 * passing things down */
class ReplicationBackend : public ThrudocPassthruBackend
{
    public:
        ReplicationBackend (boost::shared_ptr<ThrudocBackend> backend,
                            const std::string & replication_name,
                            const std::string & replication_private_name,
                            const std::string & replication_group,
                            const std::string & replication_status_file,
                            const int replication_status_flush_frequency);
        ~ReplicationBackend ();

        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        std::string admin (const std::string & op, const std::string & data);
        virtual void validate (const std::string & bucket,
                               const std::string * key,
                               const std::string * value);

    protected:
        std::string generate_uuid ();

    private:
        static log4cxx::LoggerPtr logger;

        static void * start_listener_thread (void * ptr);
        static bool orig_message_callback (Spread * /* spread_connection */,
                                           const std::string & sender,
                                           const std::vector<std::string> & groups,
                                           const int message_type,
                                           const char * message,
                                           const int message_len, void * data)
        {
            return ((ReplicationBackend*)data)->handle_orig_message
                (sender, groups, message_type, message, message_len);
        }
        static bool replay_message_callback (Spread * /* spread_connection */,
                                             const std::string & sender,
                                             const std::vector<std::string> & groups,
                                             const int message_type,
                                             const char * message,
                                             const int message_len, void * data)
        {
            return ((ReplicationBackend*)data)->handle_replay_message
                (sender, groups, message_type, message, message_len);
        }

        Spread spread;
        std::string replication_group;
        std::string current_replay_name;
        std::string replication_status_file;
        int replication_status_flush_frequency;
        pthread_t listener_thread;
        bool listener_thread_go;
        bool listener_live;
        std::string last_uuid;
        std::queue<Message *> pending_messages;

        std::map<std::string, boost::shared_ptr<ReplicationWait> >
            pending_waits;
        facebook::thrift::concurrency::ReadWriteMutex pending_waits_mutex;

        void listener_thread_run ();
        std::string send_orig_message_and_wait 
            (std::string method, std::map<std::string, std::string> params);
        void request_next (std::string message);
        void do_message (Message * message);
        bool handle_orig_message (const std::string & sender,
                                  const std::vector<std::string> & groups,
                                  const int message_type, const char * message,
                                  const int message_len);
        bool handle_replay_message (const std::string & sender,
                                    const std::vector<std::string> & groups,
                                    const int message_type, 
                                    const char * message, 
                                    const int message_len);
};

#endif /* HAVE_LIBSPREAD && HAVE_LIBUUID */

#endif /* _REPLICIATION_BACKEND_H_ */
