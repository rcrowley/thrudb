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
#include "Thrudoc.h"

using namespace thrudoc;
using namespace std;
using namespace boost;

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace facebook::thrift::concurrency;

struct eqstr {
  bool operator()(const char* s1, const char* s2) const {
    return strcmp(s1, s2) == 0;
  }
};

struct ltstr {
  bool operator()(const char* s1, const char* s2) const {
    return strcmp(s1, s2) < 0;
  }
};



// typedef hash_map<const char*, int, hash<const char*>, eqstr> count_map;
typedef map<const char*, int, ltstr> count_map;


class ClientThread: public Runnable {
public:

  ClientThread(shared_ptr<TTransport>transport, shared_ptr<ThrudocClient> client, Monitor& monitor, size_t& workerCount, size_t loopCount, int loopType) :
    _transport(transport),
    _client(client),
    _monitor(monitor),
    _workerCount(workerCount),
    _loopCount(loopCount),
    _loopType(loopType)
  {}

  void run() {

    // Wait for all worker threads to start

    {Synchronized s(_monitor);
        while(_workerCount == 0) {
          _monitor.wait();
        }
    }

    _startTime = Util::currentTime();

    _transport->open();

    //switch(_loopType) {
    loopEchoString();
    //    case T_STRING: loopEchoString(); break;
    //    default: cerr << "Unexpected loop type" << _loopType << endl; break;
    //}

    _endTime = Util::currentTime();

    _transport->close();

    _done = true;

    cerr<<"done"<<endl;

    {Synchronized s(_monitor);

      _workerCount--;

      if (_workerCount == 0) {

        _monitor.notify();
      }
    }
  }


  void loopEchoString() {
    for (size_t ix = 0; ix < _loopCount; ix++) {

        string result, id;
        string val = "HDJHSDJHSDJHJDS";
        _client->store(result,val,id);

    }
  }

  shared_ptr<TTransport> _transport;
  shared_ptr<ThrudocClient> _client;
  Monitor& _monitor;
  size_t& _workerCount;
  size_t _loopCount;
  int _loopType;
  long long _startTime;
  long long _endTime;
  bool _done;
  Monitor _sleep;
};


int main(int argc, char **argv) {


    int port = 9091;
    size_t workerCount = 4;
    size_t clientCount = 20;
    size_t loopCount = 10;
    int loopType = 0;

    ostringstream usage;

  usage <<
    argv[0] << " [--port=<port number>] [--workers=<worker-count>] [--clients=<client-count>] [--loop=<loop-count>] [--help]" << endl <<
    "\tclients        Number of client threads to create - 0 implies no clients, i.e. server only.  Default is " << clientCount << endl <<
    "\thelp           Prints this help text." << endl <<
    "\tloop           The number of remote thrift calls each client makes.  Default is " << loopCount << endl <<
    "\tport           The port the server and clients should bind to for thrift network connections.  Default is " << port << endl <<
    "\tworkers        Number of thread pools workers.  Only valid for thread-pool server type.  Default is " << workerCount << endl;


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

    if (!args["workers"].empty()) {
      workerCount = atoi(args["workers"].c_str());
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

    /*if (callName == "echoVoid") { loopType = T_VOID;}
    else if (callName == "echoByte") { loopType = T_BYTE;}
    else if (callName == "echoI32") { loopType = T_I32;}
    else if (callName == "echoI64") { loopType = T_I64;}
    else if (callName == "echoString") { loopType = T_STRING;}
    else {throw invalid_argument("Unknown service call "+callName);}*/

    for (size_t ix = 0; ix < clientCount; ix++) {

      shared_ptr<TSocket> socket(new TSocket("127.0.0.1", port));
      shared_ptr<TFramedTransport> bufferedSocket(new TFramedTransport(socket));
      shared_ptr<TProtocol> protocol(new TBinaryProtocol(bufferedSocket));
      shared_ptr<ThrudocClient> serviceClient(new ThrudocClient(protocol));

      clientThreads.insert(threadFactory->newThread(shared_ptr<ClientThread>(new ClientThread(socket, serviceClient, monitor, threadCount, loopCount, loopType))));
    }

    for (std::set<shared_ptr<Thread> >::const_iterator thread = clientThreads.begin(); thread != clientThreads.end(); thread++) {
      (*thread)->start();
    }

    long long time00;
    long long time01;

    {Synchronized s(monitor);
      threadCount = clientCount;

      cerr << "Launch "<< clientCount << " client threads" << endl;

      time00 =  Util::currentTime();

      monitor.notifyAll();

      while(threadCount > 0) {
        monitor.wait();
      }

      time01 =  Util::currentTime();
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


    cout <<  "workers :" << workerCount << ", client : " << clientCount << ", loops : " << loopCount << ", rate : " << (clientCount * loopCount * 1000) / ((double)(time01 - time00)) << endl;

    /*  count_map count = serviceHandler->getCount();
    count_map::iterator iter;
    for (iter = count.begin(); iter != count.end(); ++iter) {
      printf("%s => %d\n", iter->first, iter->second);
      }*/
    cerr << "done." << endl;
  }

  return 0;
}
