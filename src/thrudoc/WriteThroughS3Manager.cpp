#include "WriteThroughS3Manager.h"
#include "RecoveryManager.h"

using namespace std;
using namespace boost;
using namespace facebook::thrift;
using namespace log4cxx;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>


#include "ThrudocHandler.h"
#include "ThrudocSimpleWriteThroughBackend.h"
#include "Redo_types.h"

_WriteThroughS3Manager* _WriteThroughS3Manager::pInstance = 0;
Mutex                   _WriteThroughS3Manager::_mutex    = Mutex();
LoggerPtr               _WriteThroughS3Manager::logger(Logger::getLogger("WriteThroughS3Manager"));

struct null_deleter
{
    void operator()(void const *) const {}
};

class CatchupCollector : virtual public RedoIf
{
public:
    CatchupCollector():tracking(false){};

    void        log(const RedoMessage &m){
        if(tracking)
            disk_cache[m.transaction_id] = true;
    }

    void        s3log(const S3Message &m){
        tracking = true;
        s3_cache[m.transaction_id] = true;
    }

    map<string,bool>   disk_cache;
    map<string,bool> s3_cache;
private:
    bool tracking;
};


class CatchupHandler : virtual public RedoIf
{
public:
    CatchupHandler( map<string,bool> &_s3_cache, map<string,bool> &_disk_cache ) : tailing(false) {
        s3_cache    = _s3_cache;
        disk_cache  = _disk_cache;

        backend   = boost::shared_ptr<ThrudocBackend>(new ThrudocSimpleWriteThroughBackend());
        handler   = boost::shared_ptr<ThrudocHandler>(new ThrudocHandler(backend));
        processor = boost::shared_ptr<ThrudocProcessor>(new ThrudocProcessor(handler));

    }

    CatchupHandler( ) : tailing(true) {

        backend   = boost::shared_ptr<ThrudocBackend>(new ThrudocSimpleWriteThroughBackend());
        handler   = boost::shared_ptr<ThrudocHandler>(new ThrudocHandler(backend));
        processor = boost::shared_ptr<ThrudocProcessor>(new ThrudocProcessor(handler));

    }

    void        log(const RedoMessage &m){

        //If we aren't tail the log then we need to check if this
        //message is one we haven't added to s3 yet
        if(!tailing){

            if( disk_cache.count(m.transaction_id) == 0)
                return; //this transaction isn't in play

            if( s3_cache.count(m.transaction_id) > 0 )
                return; //this transaction was sent to s3
        }

        boost::shared_ptr<TTransport> buf(new TMemoryBuffer( m.message) );
        boost::shared_ptr<TProtocol> prot = protocol_factory.getProtocol(buf);

        try {

            processor->process(prot, prot);

            int32_t rseqid = 0;
            std::string fname;
            facebook::thrift::protocol::TMessageType mtype;

            //Make sure the call succeded before adding to redo log
            prot->readMessageBegin(fname, mtype, rseqid);
            if (mtype == facebook::thrift::protocol::T_EXCEPTION) {
                //cerr<<"Exception (S3 connectivity?)"<<endl;
            } else {
                //cerr<<"Processed "+m.transaction_id<<endl;
                RecoveryManager->addS3(m.transaction_id);
            }

        } catch (TTransportException& ttx) {
            cerr << "client died: " << ttx.what() << endl;
            throw;
        } catch (TException& x) {
            cerr << "exception: " << x.what() << endl;
            throw;
        } catch (...) {
            cerr << "uncaught exception." << endl;
            throw;
        }
    }

    void      s3log(const S3Message &m){
        //skip these
    }


private:
    //write threads happen locally
    TBinaryProtocolFactory                    protocol_factory;

    boost::shared_ptr<ThrudocBackend>         backend;
    boost::shared_ptr<ThrudocHandler>         handler;
    boost::shared_ptr<ThrudocProcessor>       processor;

    map<string,bool> s3_cache, disk_cache;
    bool             tailing;
};


_WriteThroughS3Manager* _WriteThroughS3Manager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _WriteThroughS3Manager();

            //Validate current state
            pInstance->startup();

            shared_ptr<PosixThreadFactory> threadFactory =
                shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

            shared_ptr<Thread> thread =
                threadFactory->newThread(shared_ptr<_WriteThroughS3Manager>(pInstance,null_deleter()));

            thread->start();
        }
    }

    return pInstance;
}


void _WriteThroughS3Manager::startup()
{
    if(started)
        return;


    string redo_log_file = RecoveryManager->getRedoLogfileName();

    boost::shared_ptr<CatchupCollector> cc(new CatchupCollector());

    //First we replay the redo log and collect the transactions that weren't sent to s3
    {
        //init new log reader
        shared_ptr<TFileTransport>           rlog( new TFileTransport(redo_log_file,true) );
        boost::shared_ptr<RedoProcessor>     proc( new RedoProcessor(cc));
        shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());

        TFileProcessor fileProcessor(proc,pfactory,rlog);

        fileProcessor.process(0,false);
    }

    char buf[256];

    sprintf(buf,"Disk/S3 [%d/%d]",cc->disk_cache.size(),cc->s3_cache.size());

    LOG4CXX_DEBUG(logger,string("Collected ")+buf);

    boost::shared_ptr<CatchupHandler>   ch(new CatchupHandler(cc->s3_cache, cc->disk_cache));

    //Now replay the log again adding the missed items to s3
    {
        //init new log reader
        shared_ptr<TFileTransport>           rlog( new TFileTransport(redo_log_file,true) );
        boost::shared_ptr<RedoProcessor>     proc( new RedoProcessor(ch));
        shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());

        TFileProcessor fileProcessor(proc,pfactory,rlog);

        fileProcessor.process(0,false);
    }


    started = true;

}

void _WriteThroughS3Manager::run()
{

    string redo_log_file = RecoveryManager->getRedoLogfileName();


    boost::shared_ptr<CatchupHandler>    ch(new CatchupHandler());
    boost::shared_ptr<RedoProcessor>     proc( new RedoProcessor(ch));
    shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());


    //init new log reader
    shared_ptr<TFileTransport>           rlog( new TFileTransport(redo_log_file,true) );
    rlog->seekToEnd();


    TFileProcessor fileProcessor(proc,pfactory,rlog);
    fileProcessor.process(0,true);

}
