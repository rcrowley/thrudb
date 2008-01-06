/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

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
#include "BDBBackend.h"
#include "DiskBackend.h"
#include "MemcachedBackend.h"
#include "MySQLBackend.h"
#include "S3Backend.h"
#include "s3_glue.h"
#include "SpreadBackend.h"
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

        string which = ConfigManager->read<string> ("BACKEND", "mysql");
        if (0)
        {
            // just here to keep thing from blowing up
        }
#if HAVE_LIBDB_CXX
        else if (which == "bdb")
        {
            // BDB backend
            string bdb_home =
                ConfigManager->read<string>("BDB_HOME", "/tmp/bdbs");
            backend =
                shared_ptr<DistStoreBackend>(new BDBBackend (bdb_home,
                                                             thread_count));
        }
#endif /* HAVE_LIBDB_CXX */
#if HAVE_LIBBOOST_FILESYSTEM
        else if (which == "disk")
        {
            // Disk backend
            string doc_root =
                ConfigManager->read<string>("DISK_DOC_ROOT", "/tmp/docs");
            backend = shared_ptr<DistStoreBackend>(new DiskBackend (doc_root));
        }
#endif /* HAVE_LIBBOOST_FILESYSTEM */
#if HAVE_LIBEXPAT && HAVE_LIBCURL
        else if (which == "s3")
        {
            // S3 backend
            curl_global_init(CURL_GLOBAL_ALL);

            // TODO: make these part of the backend, so that they're not global

            //s3_debug = 4;
            aws_access_key_id     = ConfigManager->read<string>("AWS_ACCESS_KEY").c_str();
            aws_secret_access_key = ConfigManager->read<string>("AWS_SECRET_ACCESS_KEY").c_str();

            backend = shared_ptr<DistStoreBackend>(new S3Backend ());
        }
#endif /* HAVE_LIBEXPAT && HAVE_LIBCURL */
#if HAVE_LIBMYSQLCLIENT_R
        else if (which == "mysql")
        {
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
            int max_value_size = 
                ConfigManager->read<int>("MYSQL_MAX_VALUES_SIZE", 1024);
            backend = shared_ptr<DistStoreBackend>
                (new MySQLBackend (master_hostname, master_port, master_db,
                                   username, password, max_value_size));
        }
#endif /* HAVE_LIBMYSQLCLIENT_R */
        else
        {
            LOG4CXX_ERROR (logger, string ("unknown or unbuilt backend=") + 
                           which);
            fprintf (stderr, "unknown or unbuilt backend=%s\n", 
                     which.c_str ());
            exit (1);
        }

        // Memcached cache
        string memcached_servers =
            ConfigManager->read<string>("MEMCACHED_SERVERS", "");
#if HAVE_LIBMEMCACHED
        if (!memcached_servers.empty ())
            backend = shared_ptr<DistStoreBackend>
                (new MemcachedBackend (memcached_servers, backend));
#else
        if (!memcached_servers.empty ())
        {
            LOG4CXX_ERROR (logger, "MEMCACHED_SERVERS supplied, but memcached support not complied in");
            fprintf (stderr, "MEMCACHED_SERVERS supplied, but memcached support not complied in\n");
            exit (1);
        }
#endif /* HAVE_LIBMEMCACHED */

        // Spread passthrough
        string spread_private_name =
            ConfigManager->read<string>("SPREAD_PRIVATE_NAME", "");
#if HAVE_LIBSPREAD
        string spread_name =
            ConfigManager->read<string>("SPREAD_NAME", "4803");
        string spread_group =
            ConfigManager->read<string>("SPREAD_GROUP", "diststore");

        if (!spread_private_name.empty ())
            backend = shared_ptr<DistStoreBackend>
                (new SpreadBackend (spread_name, spread_private_name,
                                    spread_group, backend));
#else
        if (!spread_private_name.empty ())
        {
            LOG4CXX_ERROR (logger, "SPREAD_PRIVATE_NAME supplied, but spread support not complied in");
            fprintf (stderr, "SPREAD_PRIVATE_NAME supplied, but spread support not complied in\n");
            exit (1);
        }
#endif /* HAVE_LIBSPREAD */

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
