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

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <stdio.h>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <EventLog.h>

#include "app_helpers.h"
#include "ConfigFile.h"
#include "ThrudocBackend.h"
#include "ThrudocHandler.h"
#include "ThrudocHandler.h"

using namespace boost;
using namespace thrudoc;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::server;
using namespace facebook::thrift::transport;
using namespace log4cxx;
using namespace log4cxx::helpers;
using namespace std;

LoggerPtr logger (Logger::getLogger ("Thrudoc"));

//print usage and die
inline void usage ()
{
    cerr<<"thrudoc -f /path/to/thrudoc.conf"<<endl;
    cerr<<"\tor create ~/.thrudoc"<<endl;
    cerr<<"\t-nb creates non-blocking server"<<endl;
    exit (-1);
}

class Replayer : public EventLogIf
{
    public:
        Replayer (shared_ptr<ThrudocBackend> backend) 
        {
            this->backend = backend;
            this->handler = shared_ptr<ThrudocHandler>
                (new ThrudocHandler (this->backend));
            this->processor = shared_ptr<ThrudocProcessor>
                (new ThrudocProcessor (this->handler));
        }

        void log (const Event & event)
        {
            char buf[1024];
            sprintf (buf, "log: event.timestamp=%ld, event.msg=***", 
                     event.timestamp); // TODO: , event.message.c_str ());
            LOG4CXX_DEBUG (logger, buf);

            shared_ptr<TTransport> tbuf(new TMemoryBuffer (event.message));
            shared_ptr<TProtocol> prot = protocol_factory.getProtocol (tbuf);

            // TODO: this isn't really "correct" error handling at all, 
            // failures are ignored, nothing is logged, etc.
            try 
            {
                processor->process(prot, prot);

                int32_t rseqid = 0;
                std::string fname;
                facebook::thrift::protocol::TMessageType mtype;

                //Make sure the call succeded before adding to redo log
                prot->readMessageBegin(fname, mtype, rseqid);
                if (mtype == facebook::thrift::protocol::T_EXCEPTION)
                {
                    //cerr<<"Exception (S3 connectivity?)"<<endl;
                }
                else 
                {
                    //cerr<<"Processed "+m.transaction_id<<endl;
                }
            } 
            catch (TTransportException& ttx) 
            {
                cerr << "client died: " << ttx.what() << endl;
                throw;
            } 
            catch (TException& x) 
            {
                cerr << "exception: " << x.what() << endl;
                throw;
            } 
            catch (...) 
            {
                cerr << "uncaught exception." << endl;
                throw;
            }
        }

    private:
        static log4cxx::LoggerPtr logger;

        TBinaryProtocolFactory protocol_factory;
        shared_ptr<ThrudocBackend> backend;
        shared_ptr<ThrudocHandler> handler;
        shared_ptr<ThrudocProcessor> processor;
};

LoggerPtr Replayer::logger (Logger::getLogger ("Replayer"));

int main (int argc, char **argv)
{

    string conf_file = string (getenv ("HOME"))+"/.thrudoc";
    bool nonblocking = true;

    //Parse args
    for (int i=0; i<argc; i++)
    {
        if (string (argv[i]) == "-f" && (i+1) < argc)
            conf_file = argv[i+1];

        if (string (argv[i]) == "-nb")
            nonblocking = true;
    }

    //Read da config
    ConfigManager->readFile ( conf_file );

    try{
        //Init logger
        PropertyConfigurator::configure (conf_file);

        LOG4CXX_INFO (logger, "Starting up");

        //Startup Services
        shared_ptr<TProtocolFactory>
            protocolFactory (new TBinaryProtocolFactory ());

        string which = ConfigManager->read<string> ("BACKEND", "mysql");

        shared_ptr<ThrudocBackend> backend = create_backend (which, 1);

        boost::shared_ptr<Replayer> replayer (new Replayer (backend));

        shared_ptr<TFileTransport> 
            rlog (new TFileTransport ("logs/thrudoc.log", true));
        boost::shared_ptr<EventLogProcessor> 
            proc (new EventLogProcessor (replayer));
        shared_ptr<TProtocolFactory> pfactory (new TBinaryProtocolFactory ());

        TFileProcessor fileProcessor (proc, pfactory, rlog);

        fileProcessor.process (0, true);

    }
    catch (TException e)
    {
        cerr << "Thrift Error: " << e.what () << endl;
    }
    catch (std::runtime_error e)
    {
        cerr << "Runtime Exception: " << e.what () << endl;
    }
    catch (ConfigFile::file_not_found e)
    {
        cerr << "ConfigFile Fatal Exception: " << e.filename << endl;
    }
    catch (ConfigFile::key_not_found e)
    {
        cerr << "ConfigFile Missing Required Key: " << e.key << endl;
    }
    catch (std::exception e)
    {
        cerr << "Caught Fatal Exception: " << e.what () << endl;
    }
    catch (...)
    {
        cerr << "Caught unknown exception" << endl;
    }

    return 0;
}
