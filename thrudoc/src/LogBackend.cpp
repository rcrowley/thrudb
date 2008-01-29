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
    {
        char buf[32];
        sprintf (buf, "%u", max_ops);
        LOG4CXX_INFO (logger, "LogBackend: log_directory=" + log_directory + 
                      ", max_ops=" + buf); 
    }

    this->set_backend (backend);
    this->log_directory = log_directory;
    this->num_ops = 0;
    this->max_ops = max_ops;

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
        log_client->send_nextLog (new_log_filename);
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
}

LogBackend::~LogBackend ()
{
    // TODO: pretty sure the flushes aren't necessary, confirm and remove
    log_transport->flush ();
    log_transport->close ();
    index_file.flush ();
    index_file.close ();
}

void LogBackend::put (const string & bucket, const string & key,
          const string & value)
{
    this->get_backend ()->put (bucket, key, value);

    //Create raw message 
    msg_client->send_put (bucket, key, value);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
}

void LogBackend::remove (const std::string & bucket, const std::string & key)
{
    this->get_backend ()->remove (bucket, key);

    //Create raw message
    msg_client->send_remove (bucket, key);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
}

string LogBackend::admin (const std::string & op, const std::string & data)
{
    if (op == "flush_log")
    {
        this->flush_log ();
        return "done";
    }
    else
    {
        string ret = this->get_backend ()->admin (op, data);

        //Create raw message
        msg_client->send_admin (op, data);
        string raw_msg = msg_transport->getBufferAsString ();
        msg_transport->resetBuffer ();

        send_message (raw_msg);

        return ret;
    }
}

vector<ThrudocException> LogBackend::putList (const vector<Element> & elements)
{
    vector<ThrudocException> ret = this->get_backend ()->putList (elements);

    //Create raw message 
    msg_client->send_putList (elements);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
    return ret;
}

vector<ThrudocException> LogBackend::removeList(const vector<Element> & elements)
{
    vector<ThrudocException> ret = this->get_backend ()->removeList (elements);

    //Create raw message 
    msg_client->send_removeList (elements);
    string raw_msg = msg_transport->getBufferAsString ();
    msg_transport->resetBuffer ();

    send_message (raw_msg);
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
    log_transport = shared_ptr<TFileTransport>
        (new TFileTransport (log_directory + "/" + log_filename));
    // hack from jake that will keep me from seeing 2-4 second hangs on send_*
    // thanks to transport's flushing
    log_transport->setFlushMaxUs (100);
    shared_ptr<TProtocol> log_protocol (new TBinaryProtocol (log_transport));
    log_client = shared_ptr<EventLogClient> (new EventLogClient (log_protocol));

    // start writing at the end, append
    log_transport->seekToEnd ();
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

void LogBackend::send_message (string raw_message)
{
    Guard g(log_mutex); 

    log_client->send_log (this->createEvent (raw_message));

    // this is going to be fuzzy b/c of multi-thread, but that's ok
    this->num_ops++;

    if (this->num_ops >= this->max_ops)
    {
        this->flush_log ();
        this->num_ops = 0;
    }
}

void LogBackend::flush_log ()
{
    string new_log_filename = get_log_filename ();
    LOG4CXX_INFO (logger, "flush_log: new logfile=" + new_log_filename);

    // time for a new file
    // point to it in the old one
    log_client->send_nextLog (new_log_filename);
    // and open the new one
    open_log_client (new_log_filename);
    // add it to the index
    index_file.write ((new_log_filename + "\n").c_str (), 
                      new_log_filename.length () + 1);
    index_file.flush ();

    // just in case we happen to reopen rather than create new
    log_transport->seekToEnd ();
}

#endif /* HAVE_LIBBOOST_FILESYSTEM */
