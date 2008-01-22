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
#include "Redo.h"

#include <stdexcept>

using namespace boost;
using namespace std;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace thrudoc;
using namespace log4cxx;

LoggerPtr LogBackend::logger (Logger::getLogger ("LogBackend"));

LogBackend::LogBackend(const string &log_directory, shared_ptr<ThrudocBackend> backend)
{

    LOG4CXX_INFO (logger, "LogBackend: LogDir=" + log_directory);

    //Verify log directory
    if(!directory_exists( log_directory )){
        LOG4CXX_ERROR (logger, string ("Invalid Log Directory: ") + log_directory );

        throw runtime_error(string ("Invalid Log Directory: ") + log_directory );
    }

    this->log_directory = log_directory;
    this->backend       = backend;

    //Serializes messages for the log
    transport = shared_ptr<TMemoryBuffer>(new TMemoryBuffer());
    shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));
    faux_client = shared_ptr<ThrudocClient>(new ThrudocClient(protocol));


    //Open logfile
    thrudoc_log = shared_ptr<TFileTransport>( new TFileTransport(log_directory + "/thrudoc.log") );

    shared_ptr<TProtocol>  log_protocol(new TBinaryProtocol(thrudoc_log));
    log_client = shared_ptr<RedoClient>( new RedoClient(log_protocol) );

    thrudoc_log->seekToEnd();

}

LogBackend::~LogBackend()
{
    thrudoc_log->flush();
}

vector<string> LogBackend::getBuckets()
{
    return backend->getBuckets();
}

string LogBackend::get (const string & bucket, const string & key)
{
    return backend->get(bucket,key);
}


void LogBackend::put (const string & bucket, const string & key,
          const string & value)
{

    backend->put(bucket,key,value);


    //Create raw message
    faux_client->send_put(bucket,key,value);
    string raw_msg = transport->getBufferAsString();
    transport->resetBuffer();

    log_client->send_log( this->createLogMessage(raw_msg) );
}

void LogBackend::remove (const std::string & bucket, const std::string & key)
{

    backend->remove(bucket,key);


    //Create raw message
    faux_client->send_remove(bucket,key);
    string raw_msg = transport->getBufferAsString();
    transport->resetBuffer();

    log_client->send_log( this->createLogMessage( raw_msg ) );

}

ScanResponse LogBackend::scan (const string & bucket,
                               const string & seed, int32_t count)
{
    return backend->scan(bucket, seed, count);
}

string LogBackend::admin (const std::string & op, const std::string & data)
{
    return backend->admin(op, data);
}


void LogBackend::validate (const std::string & bucket, const std::string * key,
                           const std::string * value)
{
    backend->validate(bucket, key, value);
}


RedoMessage LogBackend::createLogMessage(const string &msg)
{
    RedoMessage m;

    m.message         = msg;
    m.timestamp       = time(NULL);
    m.transaction_id  = generateUUID();

    return m;
}
