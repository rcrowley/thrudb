/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "RecoveryManager.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

#include "Redo_types.h"

#include <stdexcept>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdlib.h>

using namespace std;
using namespace boost;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;


_RecoveryManager* _RecoveryManager::pInstance = 0;
Mutex             _RecoveryManager::_mutex    = Mutex();



class RedoLastIdHandler : virtual public RedoIf
{
public:
    void        log(const RedoMessage &m){
        last_m = m;
    }

    void        s3log(const S3Message &m){
        //na
    }

    RedoMessage getLastMessage(){
        return last_m;
    }

private:
    RedoMessage last_m;
};


struct null_deleter
{
    void operator()(void const *) const {}
};

_RecoveryManager:: _RecoveryManager()
    : started(false)
{

}

_RecoveryManager* _RecoveryManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _RecoveryManager();

            //Validate current state
            pInstance->startup();

            shared_ptr<PosixThreadFactory> threadFactory =
                shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

            shared_ptr<Thread> thread =
                threadFactory->newThread(shared_ptr<_RecoveryManager>(pInstance,null_deleter()));


            thread->start();
        }
    }

    return pInstance;
}

void _RecoveryManager::run()
{

    while(1){

        sleep(10);



    }

}

void _RecoveryManager::startup( )
{

    if(started)
        return;

    thrudb_root = ConfigManager->read<string>("THRUDB_ROOT");
    doc_root    = ConfigManager->read<string>("DOC_ROOT");

    struct stat buffer;
    int         status;


    //Does thrudb root exist
    status = stat(thrudb_root.c_str(), &buffer);

    if(status < 0)
        throw std::runtime_error("THRUDB_ROOT is not valid (check config)");


    //Does doc root exist
    status = stat(doc_root.c_str(), &buffer);

    if(status < 0)
        throw std::runtime_error("DOC_ROOT is not valid (check config)");


    //Does snapshot script exist
    snapshot_bin = thrudb_root + "/scripts/snapshot";

    status = stat(snapshot_bin.c_str(), &buffer);

    if(status < 0)
        throw std::runtime_error("Could not locate snapshot script at: "+snapshot_bin);


    //Make sure we are uptodate
    if( ConfigManager->read<bool>("SNAPSHOT_ENABLED") && !this->isLatestSnapshot() ){
        throw std::runtime_error("This instance is out of date, please manually update it");
    }

    redo_log_file = doc_root + "/redo.log";

    //Open redo log
    redo_log = shared_ptr<TFileTransport>( new TFileTransport(redo_log_file) );
    //redo_log->setChunkSize(2 * 1024 * 1024);

    redo_log->seekToEnd();

    started = true;

}

bool  _RecoveryManager::isLatestSnapshot()
{
    string f   = ConfigManager->getConfigFilename();

    string cmd = snapshot_bin + " --config="+ f + " --uptodate";

    int status = system( cmd.c_str() );

    if( WEXITSTATUS( status ) == 0){
        return true;
    }

    return false;
}

void _RecoveryManager::addRedo(const string &msg, string transaction_id )
{
    //Create redo message and stick it into the log
    RedoMessage m;
    m.message        = msg;
    m.transaction_id = transaction_id;
    m.timestamp      = time(NULL);

    //Init faux client
    boost::shared_ptr<TMemoryBuffer> b(new TMemoryBuffer());
    boost::shared_ptr<TProtocol>     p(new TBinaryProtocol(b));
    boost::shared_ptr<RedoClient>    c(new RedoClient(p));

    //get raw message
    c->send_log(m);
    string s = b->getBufferAsString();


    //write it to log
    redo_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );
}

void _RecoveryManager::addS3(string transaction_id )
{
    S3Message m;

    m.transaction_id = transaction_id;
    m.timestamp      = time(NULL);


    //Init faux client
    boost::shared_ptr<TMemoryBuffer> b(new TMemoryBuffer());
    boost::shared_ptr<TProtocol>     p(new TBinaryProtocol(b));
    boost::shared_ptr<RedoClient>    c(new RedoClient(p));

    //get raw message
    c->send_s3log(m);
    string s = b->getBufferAsString();


    //write it to log
    redo_log->write( (uint8_t *)s.c_str(), (uint32_t) s.length() );

}


DbState _RecoveryManager::getDbState()
{
    DbState state;

    state.last_snapshot_name  = this->getLatestSnapshotName();
    state.redo_log_size       = this->redo_log->getNumChunks();

    redo_log->flush();

    //init new log reader
    shared_ptr<TFileTransport> rlog( new TFileTransport(redo_log_file,true) );
    rlog->setChunkSize(2 * 1024 * 1024);
    rlog->seekToChunk( rlog->getNumChunks()-2 );

    //Play the log till the end to grab the last trans_id
    boost::shared_ptr<TProtocol>         p(new TBinaryProtocol(rlog));
    boost::shared_ptr<RedoLastIdHandler> redoh(new RedoLastIdHandler());
    boost::shared_ptr<RedoProcessor>     proc (new RedoProcessor(redoh));

    shared_ptr<TProtocolFactory>         pfactory(new TBinaryProtocolFactory());

    TFileProcessor fileProcessor(proc,pfactory,rlog);

    //plays through log and stores last message
    fileProcessor.process(0,false);

    RedoMessage last_m = redoh->getLastMessage();

    state.last_transaction_id = last_m.transaction_id;
    state.last_timestamp      = last_m.timestamp;

    return state;
}

const string _RecoveryManager::getLatestSnapshotName()
{
    DIR *dp;
    struct dirent *ep;

    string last_snapshot_name;
    vector<string> files;

    dp = opendir ( (doc_root + "/.snapshots").c_str() );
    if (dp != NULL) {

        while ((ep = readdir (dp)))
            files.push_back(ep->d_name);

        closedir (dp);
    }

    if(files.size() > 0){
        sort(files.begin(), files.end());

        last_snapshot_name = files[ files.size()-1 ];
    }

    return last_snapshot_name;
}

void _RecoveryManager::compareDbState(const char *sender, DbState &remote_state)
{

    DbState local_state = this->getDbState();

    //Require Snapshot recovery?
    if( local_state.last_snapshot_name < remote_state.last_snapshot_name )
    {
        //Looks like we are missing some big differences... let's schedule an update
        Guard g( mutex );
        msg_queue.push_back( make_pair(SNAPSHOT_RECOVERY,remote_state.last_snapshot_name));
        return;
    }

    //Require redo log recovery?
    if( local_state.redo_log_size < remote_state.redo_log_size )
    {
        //Need to update our redo log from remote instance
        Guard g( mutex );
        msg_queue.push_back( make_pair(REDO_RECOVERY,sender));
        return;
    }

    if( local_state.last_transaction_id != remote_state.last_transaction_id  && local_state.last_timestamp != remote_state.last_timestamp )
    {
        //Need to update our redo log from remote instance
        Guard g( mutex );
        msg_queue.push_back( make_pair(REDO_RECOVERY,sender));
        return;
    }

    //Otherwise we are n-sync!
}
