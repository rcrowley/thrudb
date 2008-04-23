/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#if HAVE_LIBBOOST_FILESYSTEM

#include "LogBackend.h"
#include "utils.h"
#include <sys/time.h>
#include <stdexcept>

namespace fs = boost::filesystem;
using namespace boost;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace log4cxx;
using namespace std;
using namespace thrudex;

LoggerPtr LogBackend::logger (Logger::getLogger ("LogBackend"));

LogBackend::LogBackend (shared_ptr<ThrudexBackend> backend,
                        const string & log_directory, unsigned int max_ops,
                        unsigned int sync_wait)
{
    {
        char buf[1024];
        sprintf (buf, "LogBackend: log_directory=%s, max_ops=%u, sync_wait=%u",
                 log_directory.c_str (), max_ops, sync_wait);
        LOG4CXX_INFO (logger, buf); 
    }

    this->set_backend (backend);

    // create our serializer
    msg_transport = shared_ptr<TMemoryBuffer>(new TMemoryBuffer ());
    shared_ptr<TProtocol>  msg_protocol (new TBinaryProtocol (msg_transport));
    msg_client = shared_ptr<ThrudexClient>(new ThrudexClient (msg_protocol));

    file_logger = new FileLogger(log_directory, LOG_FILE_PREFIX, max_ops, sync_wait);
}

void LogBackend::put (const Document & d)
{
    this->get_backend ()->put (d);

    try
    {
        //Create raw message 
        msg_client->send_put (d);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        file_logger->send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("put: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudexException te;
        te.what = "partial success";
        throw te;
    }
}

void LogBackend::remove (const Element & e)
{
    this->get_backend ()->remove (e);

    try
    {
        //Create raw message
        msg_client->send_remove (e);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        file_logger->send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("remove: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudexException te;
        te.what = "partial success";
        throw te;
    }
}

string LogBackend::admin (const std::string & op, const std::string & data)
{
    if (op == "roll_log")
    {
        file_logger->roll_log ();
        return "done";
    }
    else
    {
        string ret = this->get_backend ()->admin (op, data);

        try
        {
            //Create raw message
            msg_client->send_admin (op, data);
            string raw_msg = msg_transport->getBufferAsString ();
            msg_transport->resetBuffer ();

            file_logger->send_log (raw_msg);
        }
        catch (TException e)
        {
            LOG4CXX_ERROR (logger, string ("admin: client succeeded, log failed. partial success, what=") +
                           e.what ());
            ThrudexException te;
            te.what = "partial success";
            throw te;
        }

        return ret;
    }
}

vector<ThrudexException> LogBackend::putList (const vector<Document> & documents)
{
    vector<ThrudexException> ret = this->get_backend ()->putList (documents);

    try
    {
        //Create raw message 
        msg_client->send_putList (documents);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        file_logger->send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("putList: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudexException te;
        te.what = "partial success";
        throw te;
    }

    return ret;
}

vector<ThrudexException> LogBackend::removeList(const vector<Element> & elements)
{
    vector<ThrudexException> ret = this->get_backend ()->removeList (elements);

    try
    {
        //Create raw message 
        msg_client->send_removeList (elements);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        file_logger->send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("removeList: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudexException te;
        te.what = "partial success";
        throw te;
    }

    return ret;
}

Event LogBackend::create_event (const string & message)
{
    Event event;

#define NS_PER_S 1000000000LL
#if defined(HAVE_CLOCK_GETTIME)
    struct timespec now;
    int ret = clock_gettime (CLOCK_REALTIME, &now);
    assert (ret == 0);
    event.timestamp = (now.tv_sec * NS_PER_S) + now.tv_nsec;
#elif defined(HAVE_GETTIMEOFDAY)
#define US_PER_NS 1000LL
    struct timeval now;
    int ret = gettimeofday (&now, NULL);
    assert (ret == 0);
    event.timestamp = (((int64_t)now.tv_sec) * NS_PER_S) + 
        (((int64_t)now.tv_usec) * US_PER_NS);
#else
#error "one of either clock_gettime or gettimeofday required for LogBackend"
#endif // defined(HAVE_GETTIMEDAY)

    event.message = message;

    return event;
}

#endif /* HAVE_LIBBOOST_FILESYSTEM */
