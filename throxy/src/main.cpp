#include <thrift/TProcessor.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>


#include <iostream>
#include <stdexcept>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>

#include "ConfigFile.h"
#include "utils.h"

#include "ThrudocHandler.h"
#include "StaticServiceMonitor.h"
#include "Thrudex.h"

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


LoggerPtr logger(Logger::getLogger("Throxy"));

//print usage and die
inline void usage()
{
    cerr<<"throxy -f /path/to/config.inf"<<endl;
    cerr<<"\tor create ~/.throxy"<<endl;
    exit(-1);
}

inline shared_ptr<ThrudocHandler> getThrudocHandler()
{
    if( ConfigManager->read<string>("THRUDOC_MONITOR")  == "STATIC" ){


        string conf_file = ConfigManager->read<string>("THRUDOC_MONITOR_ARG1");

        shared_ptr<ServiceMonitor> svc_mon(new StaticServiceMonitor(conf_file));

        shared_ptr<ThrudocHandler>  handler(new ThrudocHandler(svc_mon));

        return handler;
    }


    throw TException("Unknown/Missing Monitor for thrudoc");

}

int main(int argc, char **argv) {

    string conf_file   = string(getenv("HOME"))+"/.throxy";

    //Parse args
    for(int i=0; i<argc; i++){
        if(string(argv[i]) == "-f" && (i+1) < argc)
            conf_file = argv[i+1];
    }

    if( !file_exists( conf_file ) ){
        usage();
    }

    try{
        //Read da config
        ConfigManager->readFile( conf_file );

        PropertyConfigurator::configure(conf_file);

        LOG4CXX_INFO(logger, "Starting up");

        shared_ptr<PosixThreadFactory> thread_factory(new PosixThreadFactory());
        shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
        thread_factory->setDetached(false);

        //collection of server to throxy
        vector< shared_ptr<Thread> > server_threads;


        //Thrudoc enabled
        if( ConfigManager->read<int>("THRUDOC_PORT",-1) != -1 ) {

            int    thrudoc_port  = ConfigManager->read<int>("THRUDOC_PORT");
            int    thread_count  = 3;

            shared_ptr<ThrudocHandler>  handler = getThrudocHandler();
            shared_ptr<TProcessor>      processor(new thrudoc::ThrudocProcessor(handler));

            shared_ptr<ThreadManager> thread_manager =
                ThreadManager::newSimpleThreadManager(thread_count);

            shared_ptr<PosixThreadFactory> threadFactory =
                shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

            thread_manager->threadFactory(threadFactory);
            thread_manager->start();



            shared_ptr<Thread> t =
                thread_factory->newThread(shared_ptr<TServer>(new TNonblockingServer(processor, protocol_factory,
                                                                                     thrudoc_port,thread_manager)));

            server_threads.push_back(t);
        }



        //Thrudex enabled
        /*if( ConfigManager->read<int>("THRUDEX_PORT",-1) == -1 ) {

            int    thrudex_port  = ConfigManager->read<int>("THRUDEX_PORT");


            shared_ptr<ThrudocHandler>  handler          (new ThrudexHandler());
            shared_ptr<TProcessor>      processor        (new ThrudexProcessor(handler));

            shared_ptr<Thread> t = thread_factory->newThread(shared_ptr<TServer>(new TNonblockingServer(processor, protocol_factory, thrudex_port)));

            server_thread.push_back(t);
            }*/


        //Start servers
        for(size_t i=0; i<server_threads.size(); i++){
            server_threads[i]->start();
        }

        //wait for server threads
        for(size_t i=0; i<server_threads.size(); i++){
            server_threads[i]->join();
        }


    }catch(std::runtime_error e){
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }catch(std::exception e){
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }catch(ConfigFile::file_not_found e){
        cerr<<"ConfigFile Fatal Exception: "<<e.filename<<endl;
    }catch(ConfigFile::key_not_found e){
        cerr<<"ConfigFile Missing Required Key: "<<e.key<<endl;
    }catch(...){
        cerr<<"Caught unknown exception"<<endl;
    }

    return 0;
}

