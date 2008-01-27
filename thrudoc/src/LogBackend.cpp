/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "LogBackend.h"
#include "utils.h"

#include <stdexcept>

using namespace boost;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace log4cxx;
using namespace std;
using namespace thrudoc;

/*
 * TODO:
 * - error handling isn't exactly correct, backend can succeed, but log fail
 *   and an exception will be return to client, but not a very useful one, how
 *   should this behave.
 */

LoggerPtr LogBackend::logger (Logger::getLogger ("LogBackend"));

LogBackend::LogBackend (shared_ptr<ThrudocBackend> backend,
                        const string & log_directory, unsigned int max_ops)
{

    LOG4CXX_INFO (logger, "LogBackend: LogDir=" + log_directory);

    //Verify log directory
    if (!directory_exists (log_directory))
    {
        // TODO: create directory here
        LOG4CXX_ERROR (logger, string ("Invalid Log Directory: ") + log_directory);
        throw runtime_error (string ("Invalid Log Directory: ") + log_directory);
    }

    this->backend = backend;
    this->log_directory = log_directory;
    this->num_ops = 0;
    this->max_ops = max_ops;

    // Serializes messages for the log
    msg_transport = shared_ptr<TMemoryBuffer>(new TMemoryBuffer ());
    shared_ptr<TProtocol>  msg_protocol (new TBinaryProtocol (msg_transport));
    msg_client = shared_ptr<ThrudocClient>(new ThrudocClient (msg_protocol));

    // Open logfile
    string log_filename = get_log_filename ();
    LOG4CXX_INFO (logger, "LogBackend: logfile=" + log_filename);
    log_transport = shared_ptr<TFileTransport>
        (new TFileTransport (log_directory + "/" + log_filename));
    shared_ptr<TProtocol>  log_protocol (new TBinaryProtocol (log_transport));
    log_client = shared_ptr<EventLogClient> (new EventLogClient (log_protocol));

    // start writing at the end, append
    log_transport->seekToEnd ();
}

LogBackend::~LogBackend ()
{
    log_transport->flush ();
}

vector<string> LogBackend::getBuckets ()
{
    return backend->getBuckets ();
}

string LogBackend::get (const string & bucket, const string & key)
{
    return backend->get (bucket, key);
}

string LogBackend::get_log_filename ()
{
    char buf[64];
    sprintf (buf, "thrudoc.log.%d", (int)time (NULL));
    return buf; 
}

void LogBackend::send_message (string raw_message)
{
    log_client->send_log (this->createEvent (raw_message));

    // this is going to be fuzzy b/c of multi-thread, but that's ok
    this->num_ops++;

    if (this->num_ops >= this->max_ops)
    {
        // it's unlikely that a mutex at this level is good enough...
        Guard g(log_mutex); 

        string log_filename = get_log_filename ();
        LOG4CXX_INFO (logger, "send_message: new logfile=" + log_filename);

        // time for a new file
        log_transport->flush ();
        log_client->send_nextLog (log_filename);
        log_transport = shared_ptr<TFileTransport>
            (new TFileTransport (log_directory + "/" + log_filename));
        shared_ptr<TProtocol>  log_protocol (new TBinaryProtocol (log_transport));
        log_client = shared_ptr<EventLogClient> (new EventLogClient (log_protocol));

        // just in case we happen to reopen rather than create new
        log_transport->seekToEnd ();

        this->num_ops = 0;
    }
}

void LogBackend::put (const string & bucket, const string & key,
          const string & value)
{
    backend->put (bucket, key, value);

    //Create raw message 
    msg_client->send_put (bucket, key, value);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
}

void LogBackend::remove (const std::string & bucket, const std::string & key)
{
    backend->remove (bucket, key);

    //Create raw message
    msg_client->send_remove (bucket, key);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
}

ScanResponse LogBackend::scan (const string & bucket,
                               const string & seed, int32_t count)
{
    return backend->scan (bucket, seed, count);
}

string LogBackend::admin (const std::string & op, const std::string & data)
{
    string ret = backend->admin (op, data);

    //Create raw message
    msg_client->send_admin (op, data);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);

    return ret;
}

vector<ThrudocException> LogBackend::putList (const vector<Element> & elements)
{
    vector<ThrudocException> ret = backend->putList (elements);

    //Create raw message 
    msg_client->send_putList (elements);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
    return ret;
}

vector<ThrudocException> LogBackend::removeList(const vector<Element> & elements)
{
    vector<ThrudocException> ret = backend->removeList (elements);

    //Create raw message 
    msg_client->send_removeList (elements);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
    return ret;
}

void LogBackend::validate (const std::string & bucket, const std::string * key,
                           const std::string * value)
{
    backend->validate (bucket, key, value);
}


Event LogBackend::createEvent (const string & message)
{
    Event event;

#if defined(HAVE_CLOCK_GETTIME)
#define NS_PER_S 1000000000LL
    struct timespec now;
    int ret = clock_gettime (CLOCK_REALTIME, &now);
    assert (ret == 0);
    event.timestamp = (now.tv_sec * NS_PER_S) + now.tv_nsec;
#elif defined(HAVE_GETTIMEOFDAY)
#define US_PER_S 1000000LL
    struct timeval now;
    int ret = gettimeofday (&now, NULL);
    assert (ret == 0);
    event.timestamp = (((int64_t)now.tv_sec) * US_PER_S) + now.tv_usec;
#else
#error "one of either clock_gettime or gettimeofday required for LogBackend"
#endif // defined(HAVE_GETTIMEDAY)

    event.message = message;

    return event;
}
