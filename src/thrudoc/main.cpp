/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
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


#include "ThrudocDiskSpreadTask.h"
#include "ThrudocS3SpreadTask.h"
#include "ThrudocHandler.h"
#include "ThrudocDiskBackend.h"
#include "BloomManager.h"
#include "SpreadManager.h"
#include "RecoveryManager.h"
#include "ConfigFile.h"
#include "utils.h"
#include "memcache++.h"

#include "boost/shared_ptr.hpp"
#include "boost/utility.hpp"

#include "s3_glue.h"
#include "ThrudocS3Backend.h"

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

using namespace log4cxx;
using namespace log4cxx::helpers;

using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;

using namespace boost;

LoggerPtr logger(Logger::getLogger("Thrudoc"));

//print usage and die
inline void usage()
{
    cerr<<"thrudoc -f /path/to/thrudb.conf"<<endl;
    cerr<<"\tor create ~/.thrudoc"<<endl;
    cerr<<"\t-nb creates non-blocking server"<<endl;
    exit(-1);
}

int main(int argc, char **argv) {

    string conf_file = string(getenv("HOME"))+"/.thrudoc";
    bool nonblocking = true;

    //Parse args
    for(int i=0; i<argc; i++){
        if(string(argv[i]) == "-f" && (i+1) < argc)
            conf_file = argv[i+1];

        if(string(argv[i]) == "-nb")
            nonblocking = true;
    }

    if( !file_exists( conf_file ) ){
        usage();
    }


    try{
        //Read da config
        ConfigManager->readFile( conf_file );

        //Init logger
        PropertyConfigurator::configure(conf_file);

        LOG4CXX_INFO(logger, "Starting up");

        string doc_root     = ConfigManager->read<string>("DOC_ROOT");
        int    thread_count = ConfigManager->read<int>("THREAD_COUNT",3);
        int    server_port  = ConfigManager->read<int>("SERVER_PORT",9091);

        //Init the bloom filter
        BloomManager->startup();

        //Take backups
        RecoveryManager->startup();

        //Spread baby
        SpreadManager->startup();



        //Startup Services
        shared_ptr<TProtocolFactory>  protocolFactory  (new TBinaryProtocolFactory());

        shared_ptr<ThrudocBackend>     backend;


        //Which backend should we use?
        if(ConfigManager->read<string>("BACKEND_TYPE") == "S3"){
            s3::init();

            backend = boost::shared_ptr<ThrudocBackend>(new ThrudocS3Backend());
            SpreadManager->setTaskFactory( boost::shared_ptr<SpreadTaskFactory>(new ThrudocDiskSpreadTaskFactory()) );

        } else {

            backend = boost::shared_ptr<ThrudocBackend>(new ThrudocDiskBackend());
            SpreadManager->setTaskFactory( boost::shared_ptr<SpreadTaskFactory>(new ThrudocS3SpreadTaskFactory()) );

        }

        shared_ptr<ThrudocHandler>     handler          (new ThrudocHandler(backend));
        shared_ptr<ThrudocProcessor>   processor        (new ThrudocProcessor(handler));


        shared_ptr<ThreadManager> threadManager =
            ThreadManager::newSimpleThreadManager(thread_count);

        shared_ptr<PosixThreadFactory> threadFactory =
            shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

        threadManager->threadFactory(threadFactory);
        threadManager->start();


        if(nonblocking){


            TNonblockingServer server(processor,
                                      protocolFactory,
                                      server_port,threadManager);


            cerr<<"Starting the server...\n";
            server.serve();
            cerr<<"Server stopped."<<endl;

        } else {

            shared_ptr<TServerTransport>  serverTransport (new TServerSocket(server_port));
            shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

            TThreadPoolServer server(processor,
                                     serverTransport,
                                     transportFactory,
                                     protocolFactory,
                                     threadManager);


            //Try setting a default timeout...

            cerr<<"Starting the server...\n";
            server.serve();
            cerr<<"Server stopped."<<endl;
        }


    }catch(std::runtime_error e){
        cerr<<"Runtime Exception: "<<e.what()<<endl;
    }catch(ConfigFile::file_not_found e){
        cerr<<"ConfigFile Fatal Exception: "<<e.filename<<endl;
    }catch(ConfigFile::key_not_found e){
        cerr<<"ConfigFile Missing Required Key: "<<e.key<<endl;
    }catch(MemcacheException e){
        cerr<<"Caught Memcached Exception: is it running?"<<endl;
    }catch(std::exception e){
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }catch(...){
        cerr<<"Caught unknown exception"<<endl;
    }

    return 0;
}
