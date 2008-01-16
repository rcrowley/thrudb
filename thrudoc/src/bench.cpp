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

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TFileTransport.h>
#include <stdexcept>
#include <iostream>
#include "Thrudoc.h"

using namespace thrudoc;
using namespace std;
using namespace boost;

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace facebook::thrift::concurrency;

//Our "Document"
static string sample;

/**
 * Get current time as milliseconds from epoch, formerly from thrift's Util.h
 */
static const int64_t NS_PER_S = 1000000000LL;
static const int64_t MS_PER_S = 1000LL;
static const int64_t NS_PER_MS = 1000000LL;
static const int64_t currentTime() {
#if defined(HAVE_CLOCK_GETTIME)
    struct timespec now;
    int ret = clock_gettime(CLOCK_REALTIME, &now);
    assert(ret == 0);
    return
        (now.tv_sec * MS_PER_S) +
        (now.tv_nsec / NS_PER_MS) +
        (now.tv_nsec % NS_PER_MS >= 500000 ? 1 : 0) ;
#elif defined(HAVE_GETTIMEOFDAY)
    struct timeval now;
    int ret = gettimeofday(&now, NULL);
    assert(ret == 0);
    return
        (((int64_t)now.tv_sec) * MS_PER_S) +
        (now.tv_usec / MS_PER_S) +
        (now.tv_usec % MS_PER_S >= 500 ? 1 : 0);
#endif // defined(HAVE_GETTIMEDAY)
}

class ClientThread: public Runnable {
    public:

        ClientThread(shared_ptr<TTransport>transport, shared_ptr<ThrudocClient> client, Monitor& monitor, size_t& threadCount, size_t loopCount, string loopType) :
            _transport(transport),
            _client(client),
            _monitor(monitor),
            _threadCount(threadCount),
            _loopCount(loopCount),
            _loopType(loopType)
    {}

        void run() {

            // Wait for all worker threads to start
            {Synchronized s(_monitor);
                while(_threadCount == 0) {
                    _monitor.wait();
                }
            }

            _startTime = currentTime();

            _transport->open();

            if(_loopType == "read")
                testRead();
            else if(_loopType == "write")
                testWrite();
            else
                testBoth();

            _endTime = currentTime();

            _transport->close();

            _done = true;

            //cerr<<"client "<<_threadCount<<": "<<_loopType<<endl;

            {Synchronized s(_monitor);

                _threadCount--;

                if (_threadCount == 0) {

                    _monitor.notify();
                }
            }
        }


        void testWrite() {
            for (size_t ix = 0; ix < _loopCount; ix++) {
                string result, id;
                _client->putValue(id, "data",sample);

            }
        }

        void testRead() {
            for (size_t ix = 0; ix < _loopCount; ix++) {
                string result, id = "sample";
                _client->get(result,"data",id);
            }
        }

        void testBoth() {

            for (size_t ix = 0; ix < _loopCount/2; ix++) {
                string result,id;
                _client->putValue(id, "data",sample);
                _client->get(result,"data",id);
            }
        }


        shared_ptr<TTransport> _transport;
        shared_ptr<ThrudocClient> _client;
        Monitor&  _monitor;
        size_t&   _threadCount;
        size_t    _loopCount;
        string    _loopType;
        long long _startTime;
        long long _endTime;
        bool      _done;
        Monitor   _sleep;
};


int main(int argc, char **argv) {


    int    port        = 9091;

    size_t clientCount = 20;
    size_t loopCount   = 10;
    size_t docSize     = 1024;
    string type        = "both";

    ostringstream usage;

    usage <<
        argv[0] << " [--port=<port number>] [--clients=<client-count>] [--loop=<loop-count>] [--size=<doc-size>] [--type=<test-type>] [--help]" << endl <<
        "\tclients        Number of client threads to create: Default is " << clientCount << endl <<
        "\thelp           Prints this help text." << endl <<
        "\tloop           The number of remote thrift calls each client makes.  Default is " << loopCount << endl <<
        "\tport           The port the server and clients should bind to for thrift network connections.  Default is " << port << endl <<
        "\tsize           The size of the documents to create when storing. Default is " << docSize << endl <<
        "\ttype           The type of operation to test. Options are:" << endl <<
        "\t\tfetch\n\t\tstore\n\t\tboth (default)\n";


    map<string, string>  args;

    for (int ix = 1; ix < argc; ix++) {

        string arg(argv[ix]);

        if (arg.compare(0,2, "--") == 0) {

            size_t end = arg.find_first_of("=", 2);

            string key = string(arg, 2, end - 2);

            if (end != string::npos) {
                args[key] = string(arg, end + 1);
            } else {
                args[key] = "true";
            }
        } else {
            throw invalid_argument("Unexcepted command line token: "+arg);
        }
    }

    try {

        if (!args["clients"].empty()) {
            clientCount = atoi(args["clients"].c_str());
        }

        if (!args["help"].empty()) {
            cerr << usage.str();
            return 0;
        }

        if (!args["loop"].empty()) {
            loopCount = atoi(args["loop"].c_str());
        }


        if (!args["port"].empty()) {
            port = atoi(args["port"].c_str());
        }

        if(!args["size"].empty()) {
            docSize = atoi(args["size"].c_str());
        }

        if(!args["type"].empty()) {
            type = args["type"];
        }


    } catch(exception& e) {
        cerr << e.what() << endl;
        cerr << usage;
    }

    shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

    //create the document to test with
    sample.resize(docSize,'1');


    if (clientCount > 0) {

        Monitor monitor;

        size_t threadCount = 0;

        set<shared_ptr<Thread> > clientThreads;

        //Set key to read for read testing
        shared_ptr<TSocket> socket(new TSocket("127.0.0.1", port));
        shared_ptr<TFramedTransport> bufferedSocket(new TFramedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(bufferedSocket));
        shared_ptr<ThrudocClient> serviceClient(new ThrudocClient(protocol));

        socket->open();
        string r;
        serviceClient->put("data","sample",sample);

        socket->close();



        //Create client threads
        for (size_t ix = 0; ix < clientCount; ix++) {

            shared_ptr<TSocket> socket(new TSocket("127.0.0.1", port));
            shared_ptr<TFramedTransport> bufferedSocket(new TFramedTransport(socket));
            shared_ptr<TProtocol> protocol(new TBinaryProtocol(bufferedSocket));
            shared_ptr<ThrudocClient> serviceClient(new ThrudocClient(protocol));

            clientThreads.insert(threadFactory->newThread(shared_ptr<ClientThread>(new ClientThread(socket, serviceClient, monitor, threadCount, loopCount, type))));
        }

        for (std::set<shared_ptr<Thread> >::const_iterator thread = clientThreads.begin(); thread != clientThreads.end(); thread++) {
            (*thread)->start();
        }

        long long time00;
        long long time01;

        {Synchronized s(monitor);
            threadCount = clientCount;

            cerr << "Starting Benchmark...";


            time00 =  currentTime();

            monitor.notifyAll();

            while(threadCount > 0) {
                monitor.wait();
            }

            time01 =  currentTime();
        }

        long long firstTime = 9223372036854775807LL;
        long long lastTime = 0;

        double averageTime = 0;
        long long minTime = 9223372036854775807LL;
        long long maxTime = 0;

        for (set<shared_ptr<Thread> >::iterator ix = clientThreads.begin(); ix != clientThreads.end(); ix++) {

            shared_ptr<ClientThread> client = dynamic_pointer_cast<ClientThread>((*ix)->runnable());

            long long delta = client->_endTime - client->_startTime;

            assert(delta > 0);

            if (client->_startTime < firstTime) {
                firstTime = client->_startTime;
            }

            if (client->_endTime > lastTime) {
                lastTime = client->_endTime;
            }

            if (delta < minTime) {
                minTime = delta;
            }

            if (delta > maxTime) {
                maxTime = delta;
            }

            averageTime+= delta;
        }

        averageTime /= clientCount;

        cout <<" Finished"<<endl;
        cout << "clients:"<<clientCount << ", loops:"<<loopCount <<
            ", type:"<<type<<", size:"<<docSize<<", rate:" << (clientCount * loopCount * 1000) / ((double)(time01 - time00)) << endl;

    }

    return 0;
}
