/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/


#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <concurrency/Monitor.h>
#include <concurrency/Util.h>
#include <concurrency/Mutex.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <transport/TFileTransport.h>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include "Thrudex.h"
#include "utils.h"

using namespace thrudex;
using namespace std;
using namespace boost;

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace facebook::thrift::concurrency;


class ClientThread: public Runnable {
    public:

        ClientThread(shared_ptr<TTransport>transport, shared_ptr<ThrudexClient> client, Monitor& monitor, size_t& threadCount, size_t loopCount, string loopType) :
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


            try{
                _startTime = Util::currentTime();

                _transport->open();



                if(_loopType == "read")
                    testRead();
                else if(_loopType == "write")
                    testWrite();
                else
                    testBoth();


                _endTime = Util::currentTime();

                _transport->close();

            }catch(ThrudexException e){
                cerr<<"Thrudex Exception: "<<e.what<<endl;
                exit(0);
            }catch(TException e){
                cerr<<"Thrift Exception: "<<endl; //<<e.what<<endl;
                exit(0);
            }

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
            Document doc;
            doc.index = "test";

            Field f;

            f.key   = "title";
            f.value = "this is a test on thrudex";
            doc.fields.push_back( f );

            for (size_t ix = 0; ix < _loopCount; ix++) {

                doc.key   = generateUUID();

                _client->put(doc);

            }
        }

        void testRead() {
            SearchQuery q;
            q.index = "test";
            q.query = "title:thrudex";

            for (size_t ix = 0; ix < _loopCount; ix++) {
                SearchResponse r;
                _client->search(r,q);

                if(ix % 100 == 0)
                    cerr<<r.total<<endl;
            }
        }

        void testBoth() {

            Document doc;
            doc.index = "test";

            Field f;

            f.key   = "title";
            f.value = "this is a test on thrudex";
            doc.fields.push_back( f );


            SearchQuery q;
            q.index = "test";
            q.query = "title:thrudex";

            for (size_t ix = 0; ix < _loopCount/2; ix++) {
                doc.key = generateUUID();

                _client->put(doc);

                SearchResponse r;
                _client->search(r,q);
            }
        }


        shared_ptr<TTransport> _transport;
        shared_ptr<ThrudexClient> _client;
        Monitor&  _monitor;
        size_t&   _threadCount;
        size_t    _loopCount;
        string    _loopType;
        int64_t _startTime;
        int64_t _endTime;
        bool      _done;
        Monitor   _sleep;
};


int main(int argc, char **argv) {


    int    port        = 9099;

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
        "\ttype           The type of operation to test. Options are:" << endl <<
        "\t\tread\n\t\twrite\n\t\tboth (default)\n";


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



    if (clientCount > 0) {

        Monitor monitor;

        size_t threadCount = 0;

        set<shared_ptr<Thread> > clientThreads;

        //Set key to read for read testing
        shared_ptr<TSocket> socket(new TSocket("127.0.0.1", port));
        shared_ptr<TFramedTransport> bufferedSocket(new TFramedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(bufferedSocket));
        shared_ptr<ThrudexClient> serviceClient(new ThrudexClient(protocol));

        socket->open();
        string r;
        serviceClient->admin(r,string("create_index"),string("test"));
        socket->close();



        //Create client threads
        for (size_t ix = 0; ix < clientCount; ix++) {

            shared_ptr<TSocket> socket(new TSocket("127.0.0.1", port));
            shared_ptr<TFramedTransport> bufferedSocket(new TFramedTransport(socket));
            shared_ptr<TProtocol> protocol(new TBinaryProtocol(bufferedSocket));
            shared_ptr<ThrudexClient> serviceClient(new ThrudexClient(protocol));

            clientThreads.insert(threadFactory->newThread(shared_ptr<ClientThread>(new ClientThread(socket, serviceClient, monitor, threadCount, loopCount, type))));
        }

        for (std::set<shared_ptr<Thread> >::const_iterator thread = clientThreads.begin(); thread != clientThreads.end(); thread++) {
            (*thread)->start();
        }

        int64_t time00;
        int64_t time01;

        {Synchronized s(monitor);
            threadCount = clientCount;

            cerr << "Starting Benchmark...";


            time00 =  Util::currentTime();

            monitor.notifyAll();

            while(threadCount > 0) {
                monitor.wait();
            }

            time01 =  Util::currentTime();
        }

        int64_t firstTime = 9223372036854775807LL;
        int64_t lastTime = 0;

        double averageTime = 0;
        int64_t minTime = 9223372036854775807LL;
        int64_t maxTime = 0;

        for (set<shared_ptr<Thread> >::iterator ix = clientThreads.begin(); ix != clientThreads.end(); ix++) {

            shared_ptr<ClientThread> client = dynamic_pointer_cast<ClientThread>((*ix)->runnable());

            int64_t delta = client->_endTime - client->_startTime;

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
