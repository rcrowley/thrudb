/**
 *
 **/

#ifndef _REPLICIATION_RECORDER_H_
#define _REPLICIATION_RECORDER_H_

#if HAVE_LIBSPREAD && HAVE_LIBUUID

#include <EventLog_types.h>
#include <log4cxx/logger.h>
#include <vector>
#include <Spread.h>
#include <string>

#include "EventLog.h"

class ReplicationException : public std::exception
{
    public:
        ReplicationException () {}

        ReplicationException (const std::string & message) :
            message (message) {}

        ~ReplicationException () throw () {}

        std::string message;
};

typedef struct RecordedMessage;

class ReplicationRecorder
{
    public:
        ReplicationRecorder (const std::string & replication_name,
                             const std::string & replication_private_name,
                             const std::string & replication_group);

        void record ();

    private:
        static log4cxx::LoggerPtr logger;

        static bool orig_message_callback (Spread * /* spread_connection */,
                                           const std::string & sender,
                                           const std::vector<std::string> & groups,
                                           const int message_type,
                                           const char * message,
                                           const int message_len, void * data)
        {
            return ((ReplicationRecorder*)data)->handle_orig_message
                (sender, groups, message_type, message, message_len);
        }
        static bool replay_message_callback (Spread * /* spread_connection */,
                                             const std::string & sender,
                                             const std::vector<std::string> & groups,
                                             const int message_type,
                                             const char * message,
                                             const int message_len, void * data)
        {
            return ((ReplicationRecorder*)data)->handle_replay_message
                (sender, groups, message_type, message, message_len);
        }

        Spread spread;
        std::string replication_group;
        std::vector<RecordedMessage *> messages;

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

#endif /* HAVE_SPREAD && HAVE_LIBUUID */

#endif /* _REPLICIATION_RECORDER_H_ */
