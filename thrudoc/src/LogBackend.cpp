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

#if HAVE_LIBBOOST_FILESYSTEM

#include "LogBackend.h"
#include "utils.h"

#include <stdexcept>

namespace fs = boost::filesystem;
using namespace boost;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace log4cxx;
using namespace std;
using namespace thrudoc;

LoggerPtr LogBackend::logger (Logger::getLogger ("LogBackend"));

LogBackend::LogBackend (shared_ptr<ThrudocBackend> backend,
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
    this->log_directory = log_directory;
    this->num_ops = 0;
    this->max_ops = max_ops;
    this->sync_wait = sync_wait;

    // if our log directory doesn't exist, create it
    if (!fs::is_directory (log_directory))
    {
        try
        {
            fs::create_directories (log_directory);
        }
        catch (exception e)
        {
            LOG4CXX_ERROR (logger, string ("log error: ") + e.what ());
            throw e;
        }
    }

    // try opening up the index
    index_file.open (log_directory + "/" + LOG_FILE_PREFIX + "index", ios::in);

    // get the last log file name
    LOG4CXX_DEBUG (logger, "reading index");
    string old_log_filename;
    while (index_file.good ())
    {
        char row[256];
        index_file.getline (row, 256);
        if (strlen (row))
        {
            LOG4CXX_DEBUG (logger, string ("    log file=") + row);
            old_log_filename = string (row);
        }
    }
    // we're done reading
    index_file.close ();
    LOG4CXX_INFO (logger, string ("old_log_filename=") + old_log_filename);

    // our new logfile
    string new_log_filename = get_log_filename ();
    LOG4CXX_INFO (logger, string ("new_log_filename=") + new_log_filename);

    // if there's an old log
    if (!old_log_filename.empty ())
    {
        // open the old log
        open_log_client (old_log_filename);
        // write a nextLog of the new logfile so that replayers can chain
        send_nextLog (new_log_filename);
    }
    // then open the new log
    open_log_client (new_log_filename);

    // write the new logfile to the index
    index_file.open (log_directory + "/" + LOG_FILE_PREFIX + "index", 
                         ios::out | ios::app);
    if (!index_file.is_open ())
    {
        ThrudocException e;
        e.what = "error opening log index file";
        LOG4CXX_ERROR (logger, e.what);
        throw e;
    }

    index_file.write ((new_log_filename + "\n").c_str (), 
                      new_log_filename.length () + 1);

    // make sure our addition makes it to disk
    index_file.flush ();

    // create our serializer
    msg_transport = shared_ptr<TMemoryBuffer>(new TMemoryBuffer ());
    shared_ptr<TProtocol>  msg_protocol (new TBinaryProtocol (msg_transport));
    msg_client = shared_ptr<ThrudocClient>(new ThrudocClient (msg_protocol));

    // HACK: create out file serializer, TFileTransport has some flushing
    // issues, we're getting around them by using write directly, tho it's a
    // mess and the real problem there needs to be fixed. at that point this
    // stuff needs to move back to open_log_file and mem needs to be back to
    // being log...
    mem_transport = shared_ptr<TMemoryBuffer> (new TMemoryBuffer ());
    shared_ptr<TProtocol> mem_protocol (new TBinaryProtocol (mem_transport));
    mem_client = shared_ptr<EventLogClient> (new EventLogClient (mem_protocol));
}

LogBackend::~LogBackend ()
{
    log_transport->close ();
    index_file.close ();
}

void LogBackend::put (const string & bucket, const string & key,
          const string & value)
{
    this->get_backend ()->put (bucket, key, value);

    try
    {
        //Create raw message 
        msg_client->send_put (bucket, key, value);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("put: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudocException te;
        te.what = "partial success";
        throw te;
    }
}

void LogBackend::remove (const std::string & bucket, const std::string & key)
{
    this->get_backend ()->remove (bucket, key);

    try
    {
        //Create raw message
        msg_client->send_remove (bucket, key);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("remove: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudocException te;
        te.what = "partial success";
        throw te;
    }
}

string LogBackend::admin (const std::string & op, const std::string & data)
{
    if (op == "roll_log")
    {
        this->roll_log ();
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

            send_log (raw_msg);
        }
        catch (TException e)
        {
            LOG4CXX_ERROR (logger, string ("admin: client succeeded, log failed. partial success, what=") +
                           e.what ());
            ThrudocException te;
            te.what = "partial success";
            throw te;
        }

        return ret;
    }
}

vector<ThrudocException> LogBackend::putList (const vector<Element> & elements)
{
    vector<ThrudocException> ret = this->get_backend ()->putList (elements);

    try
    {
        //Create raw message 
        msg_client->send_putList (elements);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("putList: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudocException te;
        te.what = "partial success";
        throw te;
    }

    return ret;
}

vector<ThrudocException> LogBackend::removeList(const vector<Element> & elements)
{
    vector<ThrudocException> ret = this->get_backend ()->removeList (elements);

    try
    {
        //Create raw message 
        msg_client->send_removeList (elements);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        send_log (raw_msg);
    }
    catch (TException e)
    {
        LOG4CXX_ERROR (logger, string ("removeList: client succeeded, log failed. partial success, what=") +
                       e.what ());
        ThrudocException te;
        te.what = "partial success";
        throw te;
    }

    return ret;
}

string LogBackend::get_log_filename ()
{
    char buf[64];
    sprintf (buf, "%s%d", LOG_FILE_PREFIX, (int)time (NULL));
    return buf; 
}

void LogBackend::open_log_client (string log_filename)
{
    LOG4CXX_INFO (logger, "open_log_client: log_filename=" + log_filename);

    // flush old log file
    if (log_transport)
        log_transport->flush ();

    // and open up a new one
    log_transport = shared_ptr<ThruFileWriterTransport>
        (new ThruFileWriterTransport (log_directory + "/" + log_filename, 
                                      this->sync_wait));
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

void LogBackend::send_nextLog (string new_log_filename)
{
    /* HACK: to get around TFileTransport flush problems, see constructor */
    mem_client->send_nextLog (new_log_filename);
    string s = mem_transport->getBufferAsString();
    mem_transport->resetBuffer ();
    log_transport->write ((uint8_t *)s.c_str (), (uint32_t)s.length ());
}

void LogBackend::send_log (string raw_message)
{
    /* HACK: to get around TFileTransport flush problems, see constrctor */
    mem_client->send_log (this->create_event (raw_message));
    string s = mem_transport->getBufferAsString ();
    mem_transport->resetBuffer ();
    log_transport->write ((uint8_t *)s.c_str (), (uint32_t)s.length ());

    // this is going to be fuzzy b/c of multi-thread, but that's ok
    this->num_ops++;

    if (this->num_ops >= this->max_ops)
    {
        Guard g(log_mutex); 
        if (this->num_ops >= this->max_ops)
        {   
            this->roll_log ();
            this->num_ops = 0;
        }
    }
}

void LogBackend::roll_log ()
{
    string new_log_filename = get_log_filename ();
    LOG4CXX_INFO (logger, "roll_log: new logfile=" + new_log_filename);

    // time for a new file
    // point to it in the old one
    send_nextLog (new_log_filename);
    // and open the new one
    open_log_client (new_log_filename);
    // add it to the index
    index_file.write ((new_log_filename + "\n").c_str (), 
                      new_log_filename.length () + 1);
    index_file.flush ();
}

#endif /* HAVE_LIBBOOST_FILESYSTEM */
