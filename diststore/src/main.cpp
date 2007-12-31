/**
 *
 **/

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>

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

#include "ConfigFile.h"
#include "utils.h"
#include "DistStoreBackend.h"
#include "MemcachedBackend.h"
#include "MySQLBackend.h"
#include "DistStoreHandler.h"

using namespace log4cxx;
using namespace log4cxx::helpers;

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;

using namespace boost;

LoggerPtr logger (Logger::getLogger ("DistStore"));

//print usage and die
inline void usage ()
{
    cerr<<"thrudoc -f /path/to/diststore.conf"<<endl;
    cerr<<"\tor create ~/.thrudoc"<<endl;
    cerr<<"\t-nb creates non-blocking server"<<endl;
    exit (-1);
}

int main (int argc, char **argv) {

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

    if ( file_exists ( conf_file ) )
    {
        //Read da config
        ConfigManager->readFile ( conf_file );
    }

    try{
        //Init logger
        PropertyConfigurator::configure (conf_file);

        LOG4CXX_INFO (logger, "Starting up");

        int    thread_count = ConfigManager->read<int>("THREAD_COUNT",3);
        int    server_port  = ConfigManager->read<int>("SERVER_PORT",9091);

        //Startup Services
        shared_ptr<TProtocolFactory>
            protocolFactory (new TBinaryProtocolFactory ());

        shared_ptr<DistStoreBackend> backend;

        // MySQL backend
        string master_hostname =
            ConfigManager->read<string>("MYSQL_MASTER_HOST", "localhost");
        short master_port = ConfigManager->read<short>("MYSQL_MASTER_PORT",
                                                       3306);
        string master_db = ConfigManager->read<string>("MYSQL_MASTER_DB",
                                                       "diststore");
        string username = ConfigManager->read<string>("MYSQL_USERNAME",
                                                       "diststore");
        string password = ConfigManager->read<string>("MYSQL_PASSWORD",
                                                       "diststore");
        backend = shared_ptr<DistStoreBackend>
            (new MySQLBackend (master_hostname, master_port, master_db,
                               username, password));

        // Memcached cache
        string memcached_servers =
            ConfigManager->read<string>("MEMCACHED_SERVERS", "");
        if (!memcached_servers.empty ())
            backend = shared_ptr<DistStoreBackend>
                (new MemcachedBackend (memcached_servers, backend));

        shared_ptr<DistStoreHandler>   handler (new DistStoreHandler (backend));
        shared_ptr<DistStoreProcessor> processor (new DistStoreProcessor (handler));


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
        cerr<<"Caught Fatal Exception: "<<e.what ()<<endl;
    }
    catch (...)
    {
        cerr<<"Caught unknown exception"<<endl;
    }

    return 0;
}
