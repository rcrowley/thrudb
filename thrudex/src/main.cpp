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
#include <stdlib.h>

#include "RecoveryManager.h"
#include "SpreadManager.h"
#include "ThrudexHandler.h"
#include "ThrudexSpreadTask.h"
#include "LuceneManager.h"
#include "ConfigFile.h"
#include "utils.h"
#include "LOG4CXX.h"
#include "memcache++.h"

#include "log4cxx/logger.h"

using namespace log4cxx;


using namespace std;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace thrudex;
using namespace boost;

LoggerPtr logger(Logger::getLogger("Thrudex"));

//print usage and die
inline void usage()
{
    cerr<<"thrudex -f /path/to/config.inf -nb"<<endl;
    cerr<<"\tor create ~/.thrudex"<<endl;
    cerr<<"\t-nb creates non-blocking server"<<endl;
    exit(-1);
}

void read_config(int &server_port, int &thread_count, string &index_root, map<string,vector<string> > &indexes)
{
    try{
        index_root   = ConfigManager->read<string>("DOC_ROOT");
        thread_count = ConfigManager->read<int>("THREAD_COUNT",3);
        server_port  = ConfigManager->read<int>("SERVER_PORT",9090);

        char buf[32];

        for(int i=1; i<100; i++){

            sprintf(buf,"DOMAIN_%d",i);
            string domain = ConfigManager->read<string>(buf);
            sprintf(buf,"INDEX_%d",i);
            string index  = ConfigManager->read<string>(buf);

            if( domain.empty() || index.empty() ){
                return;
            } else {

                indexes[ domain ].push_back(index);
            }
        }
    }catch(...){
        //ignore...
    }
}

int main(int argc, char **argv) {

    string conf_file   = string(getenv("HOME"))+"/.thrucene";
    bool   nonblocking = true;

    //Parse args
    for(int i=0; i<argc; i++){
        if(string(argv[i]) == "-f" && (i+1) < argc)
            conf_file = argv[i+1];

        if(string(argv[i]) == "-nb"){
            nonblocking = true;
        }
    }

    if( !file_exists( conf_file ) ){
        usage();
    }

    try{
        //Read da config
        ConfigManager->readFile( conf_file );

        string                       index_root;
        map<string,vector<string> >  indexes;
        int                          thread_count;
        int                          server_port;

        read_config(server_port,thread_count,index_root,indexes);

        if(indexes.empty())
        {
            cerr<<"Bad or missing config file"<<endl;
            exit(-1);
        }

        LOG4CXX::configure(conf_file);

        setlocale(LC_CTYPE, "en_US.utf8");  //unicode support

        //This initializes the Indexes
        LuceneManager->startup(index_root, indexes);


        LOG4CXX_INFO(logger, "Starting up");


        //Take backups
        RecoveryManager->startup();

        //Spread baby
        SpreadManager->startup();


        shared_ptr<TProtocolFactory>  protocolFactory  (new TBinaryProtocolFactory());
        shared_ptr<ThrudexHandler>   handler          (new ThrudexHandler());
        shared_ptr<TProcessor>        processor        (new ThrudexProcessor(handler));


        SpreadManager->setTaskFactory( boost::shared_ptr<SpreadTaskFactory>(new ThrudexSpreadTaskFactory()) );

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

            cerr<<"Starting the server...\n";
            server.serve();
            cerr<<"Server stopped."<<endl;
        }

    }catch(std::exception e){
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }catch(ConfigFile::file_not_found e){
        cerr<<"ConfigFile Fatal Exception: "<<e.filename<<endl;
    }catch(ConfigFile::key_not_found e){
        cerr<<"ConfigFile Missing Required Key: "<<e.key<<endl;
    }catch(MemcacheException e){
        cerr<<"Memcached Exception: is it running?"<<endl;
    }catch(...){
        cerr<<"Caught unknown exception"<<endl;
    }

    LuceneManager->destroy();

    return 0;
}
