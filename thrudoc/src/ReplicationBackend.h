/**
 *
 **/

#ifndef _REPLICIATION_BACKEND_H_
#define _REPLICIATION_BACKEND_H_

#if HAVE_LIBMEMCACHED && HAVE_LIBUUID

#include <log4cxx/logger.h>
#include <queue>
#include <sp.h>
#include <string>
#include <thrift/concurrency/Mutex.h>
#include <uuid/uuid.h>

#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

class ReplicationWait;
class ReplicationMessage;

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

        std::string replication_name;
        std::string replication_private_name;
        std::string replication_group;
        std::string replication_private_group;
        std::string replication_status_file;
        int replication_status_flush_frequency;
        mailbox spread_mailbox;
        pthread_t listener_thread;
        bool listener_thread_go;
        bool listener_live;
        std::string last_uuid;
        std::queue<ReplicationMessage *> pending_messages;
        std::map<std::string, boost::shared_ptr<ReplicationWait> > 
            pending_waits;
        facebook::thrift::concurrency::ReadWriteMutex pending_waits_mutex;

        std::string send_and_wait_for_resp (const char * msg, std::string uuid);
        void listener_thread_run ();
        void handle_message ();
        void do_message (ReplicationMessage * message);
        void request_next (std::string message);
};

#endif /* HAVE_LIBMEMCACHED */

#endif /* _REPLICIATION_BACKEND_H_ */
