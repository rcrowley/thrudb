/**
 *
 **/

#ifndef __SPREAD_REPLICIATION_BACKEND_H_
#define __SPREAD_REPLICIATION_BACKEND_H_

#if HAVE_LIBMEMCACHED && HAVE_LIBUUID

#include <log4cxx/logger.h>
#include <queue>
#include <sp.h>
#include <string>
#include <thrift/concurrency/Mutex.h>
#include <uuid/uuid.h>

#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

class SpreadReplicationWait;
class SpreadReplicationMessage;

/* NOTE: this is not exactly a normal passthrough backend even tho it kinda
 * looks like one. we use the passthrough, but there's spread in between
 * passing things down */
class SpreadReplicationBackend : public ThrudocPassthruBackend
{
    public:
        SpreadReplicationBackend (boost::shared_ptr<ThrudocBackend> backend,
                                  const std::string & spread_name, 
                                  const std::string & spread_private_name,
                                  const std::string & spread_group);
        ~SpreadReplicationBackend ();

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

        std::string spread_name;
        std::string spread_private_name;
        std::string spread_group;
        std::string spread_private_group;
        mailbox spread_mailbox;
        pthread_t listener_thread;
        bool listener_thread_go;
        bool listener_live;
        std::queue<SpreadReplicationMessage *> pending_messages;
        std::map<std::string, boost::shared_ptr<SpreadReplicationWait> > 
            pending_waits;
        facebook::thrift::concurrency::ReadWriteMutex pending_waits_mutex;

        std::string send_and_wait_for_resp (const char * msg, std::string uuid);
        void listener_thread_run ();
        void handle_message ();
        void do_message (SpreadReplicationMessage * message);
        void request_next (const char * message);
};

#endif /* HAVE_LIBMEMCACHED */

#endif
