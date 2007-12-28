/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/


#include "Queue.h"
#include "ConfigFile.h"
#include "utils.h"
#include "QueueLog.h"

#include <stdexcept>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

using namespace std;
using namespace boost;
using namespace facebook::thrift;
using namespace log4cxx;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace Thruqueue;

LoggerPtr   Queue::logger(Logger::getLogger("Queue"));


class PruneCollector : virtual public QueueLogIf
{
public:
    PruneCollector() :
        dequeue_count(0),last_flush(0){};

    void log(const QueueMessage &m){

        switch( m.op ){
            case ENQUEUE:
                prune_cache[m.message_id]++; break;
            case DEQUEUE:
                prune_cache[m.message_id]--;

                //Keep tabs on number on how dirty the
                //current log is...
                dequeue_count++;
                break;
            case FLUSH:
                prune_cache.clear();
                last_flush = m.timestamp;
                break;
            default:
                break;
        };
    }

    map<string, int>  prune_cache;
    int               dequeue_count;
    time_t            last_flush;
};


class PruneHandler : virtual public QueueLogIf
{
public:
    PruneHandler(shared_ptr<TFileTransport> _prune_log, map<string, int> &_prune_cache, bool _is_unique) :
        prune_log(_prune_log), prune_cache(_prune_cache), is_unique(_is_unique), queue_length(0)
    {
        transport   = shared_ptr<TMemoryBuffer>(new TMemoryBuffer());
        shared_ptr<TProtocol>      p(new TBinaryProtocol(transport));
        faux_client = shared_ptr<QueueLogClient>(new QueueLogClient(p));
    };

    void log(const QueueMessage &m )
    {

        if(m.op == ENQUEUE && prune_cache[m.message_id] > 0 ){

            if(!is_unique || (is_unique && unique_keys.find(m.key) == unique_keys.end())){


                faux_client->send_log(m);
                string s = transport->getBufferAsString();

                transport->resetBuffer();

                //write it to log
                prune_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );

                queue_length++;

                if(is_unique)
                    unique_keys.insert(m.key);
            }
        }
    }


    shared_ptr<TFileTransport> prune_log;
    shared_ptr<QueueLogClient> faux_client;
    shared_ptr<TMemoryBuffer>  transport;

    map<string, int>  prune_cache;

    bool              is_unique;
    set<string>       unique_keys;
    unsigned int      queue_length;
};


class QueueLogReader : virtual public QueueLogIf
{
public:
    QueueLogReader( Queue *_queue )
        : queue(_queue) {};

    void log(const QueueMessage &m ){

        assert(m.op != FLUSH);  //should never encounter a flush

        if(m.op == ENQUEUE)
            queue->enqueue(m);
    }

    Queue *queue;
};


Queue::Queue(const string name, bool unique)
    : queue_name(name), is_unique(unique), queue_length(0), is_pruning(false), msg_buffer_size(200)
{
    string doc_root    = ConfigManager->read<string>("DOC_ROOT");

    if( !directory_exists(doc_root) )
        throw std::runtime_error("DOC_ROOT is not valid (check config)");

    queue_log_file = doc_root + "/"+queue_name+".log";


    //Create the faux log client
    transport = boost::shared_ptr<TMemoryBuffer>(new TMemoryBuffer());
    boost::shared_ptr<TProtocol>  p(new TBinaryProtocol(transport));
    queue_log_client             = shared_ptr<QueueLogClient>(new QueueLogClient(p));

    this->pruneLogFile();
}

void Queue::pruneLogFile()
{
    LOG4CXX_DEBUG(logger,"Entering pruneLogFile("+queue_name+")");

    //First check that log file exists
    //If it does not then just create it and leave
    if( !file_exists( queue_log_file ) ){

        LOG4CXX_DEBUG(logger,"No Log exists for this queue creating one");

        queue_log                    = shared_ptr<TFileTransport>(new TFileTransport(queue_log_file) );
        queue_log->setFlushMaxUs(100);


        //Create log processor to read from
        queue_log_reader             = shared_ptr<TFileTransport>(new TFileTransport(queue_log_file,true) );
        shared_ptr<TProtocolFactory>   pfactory(new TBinaryProtocolFactory());
        shared_ptr<QueueLogReader>     qlr(new QueueLogReader(this));
        shared_ptr<QueueLogProcessor>  proc    (new QueueLogProcessor(qlr));
        queue_log_processor          = shared_ptr<TFileProcessor>(new TFileProcessor(proc,pfactory,queue_log_reader));

        return;
    }


    //Notify other threads that we are about to prune
    {
        Guard g(mutex);
        is_pruning = true;
        pruning_queue.clear(); //just incase any are lingering

        if( queue_log != shared_ptr<TFileTransport>() ){
            queue_log->flush();
            queue_log->close();
        }
    }


    //Now prune the log
    try{

        shared_ptr<PruneCollector> pc(new PruneCollector());

        LOG4CXX_DEBUG(logger,"Collecting Messages()");

        //First we replay the queue log and collect the messages that are no longer needed
        {
            //init new log reader
            shared_ptr<TFileTransport>           plog( new TFileTransport(queue_log_file,true) );
            shared_ptr<QueueLogProcessor>        proc( new QueueLogProcessor(pc));
            shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());

            TFileProcessor fileProcessor(proc,pfactory,plog);

            fileProcessor.process(0,false);
        }


        //backup the old log
        char buf[512];
        sprintf(buf,".%d",(int)time(NULL));
        string backup_log = queue_log_file + buf;

        int rc = rename(queue_log_file.c_str(),backup_log.c_str());

        assert(rc == 0); //FIXME: better have worked

        LOG4CXX_DEBUG(logger,"Populating new log()");

        //Replay old log to create a new pruned log
        {

            //Log file to process
            shared_ptr<TFileTransport>           plog_old(new TFileTransport(backup_log,true) );


            //New log to create
            queue_log        =    shared_ptr<TFileTransport>(new TFileTransport(queue_log_file) );
            queue_log->setFlushMaxUs(100);

            //Prune handler does the work
            shared_ptr<TProtocol>                proto   (new TBinaryProtocol(plog_old));
            shared_ptr<PruneHandler>             ph      (new PruneHandler(queue_log,pc->prune_cache,is_unique));
            shared_ptr<QueueLogProcessor>        proc    (new QueueLogProcessor(ph));
            shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());

            TFileProcessor fileProcessor(proc,pfactory,plog_old);
            fileProcessor.process(0,false);

            //Log file should be pruned. now lets populate our meta info
            {
                Guard g(mutex);
                is_pruning = false;
                LOG4CXX_DEBUG(logger,"Kept Unread Messages()");
                this->queue_length = ph->queue_length;
                this->unique_keys  = ph->unique_keys;
                this->queue.clear();
            }

            //Import the new messages collected during pruning
            while(pruning_queue.size() > 0){
                string mess = pruning_queue[ pruning_queue.size()-1];
                pruning_queue.pop_back();

                this->enqueue( mess );
            }
        }

        LOG4CXX_DEBUG(logger,"Creating new log client");

        //Finally, create log processor to read from
        {
            queue_log_reader             = shared_ptr<TFileTransport>(new TFileTransport(queue_log_file,true) );
            shared_ptr<QueueLogReader>     qlr ( new QueueLogReader(this));
            shared_ptr<QueueLogProcessor>  proc( new QueueLogProcessor(qlr));
            shared_ptr<TProtocolFactory>   pfactory(new TBinaryProtocolFactory());
            queue_log_processor          = shared_ptr<TFileProcessor>(new TFileProcessor(proc,pfactory,queue_log_reader));

        }

        unlink(backup_log.c_str());

    }catch(TException e){

        LOG4CXX_ERROR(logger,e.what());

    }catch(...){
        perror("Pruning failed big time");
    }

    LOG4CXX_DEBUG(logger,"Exiting pruneLogFile()");
}

void Queue::enqueue(const QueueMessage &m)
{

    //This is called via log handler
    //FIXME: should be private with friend
    queue.push_back(m);
}

void Queue::enqueue(const string &mess, bool priority)
{
    if(mess.empty())
        return;

    Guard g(mutex);

    if(priority){
        priority_queue.push_back(mess);
        queue_length++;
        return;
    }

    if(is_pruning){
        pruning_queue.push_front(mess);
        queue_length++;
        return;
    }

    QueueMessage m;
    m.message_id = generateUUID();
    m.op         = ENQUEUE;
    m.message    = mess;

    m.key        = generateMD5(mess);

    if(!is_unique || (is_unique && unique_keys.find(m.key) == unique_keys.end() )){

        queue_log_client->send_log(m);

        string s = transport->getBufferAsString();
        transport->resetBuffer();

        //write it to log
        queue_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );

        //LOG4CXX_DEBUG(logger,"Adding Message:"+mess);

        queue_length++;
    }else{
        LOG4CXX_DEBUG(logger,"Skipping non unique message:"+mess);
    }
}

string Queue::dequeue()
{
    Guard g(mutex);

    if(queue_length == 0)
        return "";

    if(priority_queue.size() > 0){
        string mess = priority_queue[ priority_queue.size()-1 ];
        priority_queue.pop_back();

        queue_length--;

        return mess;
    }

    if(is_pruning){

        if(pruning_queue.size()){
            string mess = pruning_queue[ pruning_queue.size()-1 ];
            pruning_queue.pop_back();
        }

        return "";
    }

    if(queue.size() == 0)
        this->bufferMessagesFromLog();


    if(queue.size() > 0){

        QueueMessage m =  queue[queue.size()-1];
        queue.pop_back();

        string mess  = m.message;

        m.message = "";
        m.op      = DEQUEUE;


        queue_log_client->send_log(m);

        string s = transport->getBufferAsString();
        transport->resetBuffer();

        //write it to log
        queue_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );


        if(is_unique)
            unique_keys.erase(m.key);

        queue_length--;

        return mess;
    }

    return "";
}


string Queue::peek()
{
    Guard g(mutex);

    if(queue_length == 0)
        return "";

    if(priority_queue.size() > 0)
        return priority_queue[ priority_queue.size()-1 ];

    if(is_pruning)
        return pruning_queue.size() ? pruning_queue[priority_queue.size()-1] : "";


    if(queue.size() == 0)
        this->bufferMessagesFromLog();


    if(queue.size() > 0){
        QueueMessage m( queue.back() );

        return m.message;
    }

    return "";
}


unsigned int Queue::length()
{
    Guard g(mutex);
    return queue_length;
}

void Queue::bufferMessagesFromLog()
{

    if(queue.size() > 0)
        return;

    if(queue_length == 0)
        return;

    //Buffer a set of new items into the queue from the log
    if( queue_length <= msg_buffer_size ){

        //There's enough to fill the buffer, load it all
        queue_log_processor->process(queue_length,false);

        //Hmm, nada try flushing the log and reading again
        if(queue.size() == 0)
            queue_log->flush();

        queue_log_processor->process(queue_length,false);

        assert( queue.size() == queue_length );
    } else {

        while(queue.size() < msg_buffer_size){
            queue_log_processor->process(1,false);
        }
    }

}

void Queue::flush()
{
    Guard g(mutex);

    if(is_pruning){
        ThruqueueException e;
        e.what = "Flush failed because queue log is being pruned, try again in a few";
        throw e;
    }

    QueueMessage m;
    m.op = FLUSH;

    queue_log_client->send_log(m);

    string s = transport->getBufferAsString();
    transport->resetBuffer();

    //write it to log
    queue_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );


    queue_log->flush();

    //Skip to end of log
    queue_log_reader->seekToEnd();

    //Clear eveything
    queue_length = 0;
    queue.clear();
    priority_queue.clear();
    pruning_queue.clear();
    unique_keys.clear();
}
