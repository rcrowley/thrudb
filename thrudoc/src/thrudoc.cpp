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
//#include <protocol/TCountingProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <stdio.h>

#include "boost/shared_ptr.hpp"
#include "boost/utility.hpp"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "app_helpers.h"
#include "ConfigFile.h"
#include "ThrudocBackend.h"
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

        int    thread_count = ConfigManager->read<int>("THREAD_COUNT",3);
        int    server_port  = ConfigManager->read<int>("SERVER_PORT",9091);

        //Startup Services
        shared_ptr<TProtocolFactory>
            protocolFactory(new TBinaryProtocolFactory ());
        //shared_ptr<TProtocolFactory>
        //    protocolFactory (new TCountingProtocolFactory (protF));

        string which = ConfigManager->read<string> ("BACKEND", "mysql");

        shared_ptr<ThrudocBackend> backend = create_backend (which, 
                                                             thread_count);
        shared_ptr<ThrudocHandler>   handler (new ThrudocHandler (backend));
        shared_ptr<ThrudocProcessor> processor (new ThrudocProcessor (handler));

        shared_ptr<ThreadManager> threadManager =
            ThreadManager::newSimpleThreadManager (thread_count);

        shared_ptr<PosixThreadFactory> threadFactory =
            shared_ptr<PosixThreadFactory>(new PosixThreadFactory ());

        threadManager->threadFactory (threadFactory);
        threadManager->start ();

        if (nonblocking)
        {


            TNonblockingServer server (processor,
                                       protocolFactory,
                                       server_port,threadManager);


            cerr<<"Starting the server...\n";
            server.serve ();
            cerr<<"Server stopped."<<endl;

        } else {

            shared_ptr<TServerTransport>
                serverTransport (new TServerSocket (server_port));
            shared_ptr<TTransportFactory>
                transportFactory (new TBufferedTransportFactory ());

            TThreadPoolServer server (processor,
                                      serverTransport,
                                      transportFactory,
                                      protocolFactory,
                                      threadManager);


            //Try setting a default timeout...

            cerr<<"Starting the server...\n";
            server.serve ();
            cerr<<"Server stopped."<<endl;
        }


    }
    catch (TException e)
    {
        cerr<<"Thrift Error: "<<e.what()<<endl;
    }
    catch (std::runtime_error e)
    {
        cerr<<"Runtime Exception: "<<e.what ()<<endl;
    }
    catch (ConfigFile::file_not_found e)
    {
        cerr<<"ConfigFile Fatal Exception: "<<e.filename<<endl;
    }
    catch (ConfigFile::key_not_found e)
    {
        cerr<<"ConfigFile Missing Required Key: "<<e.key<<endl;
    }
    catch (std::exception e)
    {
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }
    catch (...)
    {
        cerr<<"Caught unknown exception"<<endl;
        }

    return 0;
}

