/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "SpreadManager.h"

#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>

#include <sp.h>
#include <log4cxx/logger.h>

#include "TransactionManager.h"
#include "MemcacheHandle.h"

using namespace std;
using namespace boost;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::server;
using namespace log4cxx;

_SpreadManager* _SpreadManager::pInstance = 0;
Mutex           _SpreadManager::_mutex    = Mutex();

#define memd    MemcacheHandle::instance()
#define OFFLINE -1
#define ONLINE   1

LoggerPtr _SpreadManager::logger(Logger::getLogger("SpreadManager"));


//Used to effectivly  shared_ptr
struct null_deleter
{
    void operator()(void const *) const {}
};

_SpreadManager* _SpreadManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _SpreadManager();

            pInstance->startup();

            LOG4CXX_INFO(logger, string("Spread Manager ") + (pInstance->isEnabled() ? "Enabled" : "Disabled"));

            //If enabled startup libevent on a new thread
            if( pInstance->isEnabled() ){
                shared_ptr<PosixThreadFactory> threadFactory =
                    shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

                shared_ptr<Thread> thread =
                    threadFactory->newThread(shared_ptr<_SpreadManager>(pInstance,null_deleter()));

                thread->start();
            }

        }
    }

    return pInstance;
}

void _SpreadManager::startup()
{
    spread_enabled = ConfigManager->read<bool>("SPREAD_ENABLED",false);

    if(spread_enabled){
        thread_factory = boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
    }

}

bool _SpreadManager::isEnabled() const
{
    return spread_enabled;
}

void _SpreadManager::run()
{

    spread_server = ConfigManager->read<string>("SPREAD_SERVER","4803@127.0.0.1");
    spread_group  = ConfigManager->read<string>("SPREAD_GROUP","THRUDB");
    spread_name   = ConfigManager->read<string>("SPREAD_NAME");

    LOG4CXX_INFO(logger,"Using name: "+spread_name);

    quorum_size   = ConfigManager->read<int>("SPREAD_QUORUM_SIZE",2);


    /* connecting to the daemon, requesting group information */
    int ret = SP_connect( spread_server.c_str(), spread_name.c_str(), 0, 1, &fd, private_group );
    if( ret < 0 )
    {
        SP_error( ret );
        exit(0);
    }

    // Initialize libevent
    event_init();

    // Register the server event
    struct event serverEvent;
    event_set(&serverEvent,
              fd,
              EV_READ | EV_PERSIST,
              _SpreadManager::eventHandler,
              this);

    // Add the event and start up the server
    if (-1 == event_add(&serverEvent, 0)) {
        cerr<<"Error adding spread libevent";
        exit(0);
    }

    //Join group
    SP_join( fd, spread_group.c_str() );

    //Kick off the messages
    this->send( spread_group.c_str(), "op:helo", true );

    //Also send the dbState
    DbState state = RecoveryManager->getDbState();
    char buf[128]; sprintf(buf,"%d %ld",state.redo_log_size,state.last_timestamp);

    this->send( spread_group.c_str(), (string("op:dbstate ")+state.last_snapshot_name+" "+buf+" "+state.last_transaction_id).c_str(), true );

    event_loop(0);
}

void _SpreadManager::readMessage()
{

    int16           mess_type;
    int             endian_mismatch;
    int             ret;

    service_type = 0;

    ret = SP_receive( fd, &service_type, sender, 100, &num_groups, target_groups,
                      &mess_type, &endian_mismatch, sizeof(mess), mess );
    if( ret < 0 )
    {

        SP_error( ret );
        exit(0);
    }

    if( Is_regular_mess( service_type ) )
    {

        mess[ret] = '\0';
        reciever =  target_groups[0];

        this->processMessage();


    } else if( Is_membership_mess( service_type ) ){

        // A membership notification
        ret = SP_get_memb_info( mess, service_type, &memb_info );

        if (ret < 0) {
            printf("BUG: membership message does not have valid body\n");
            SP_error( ret );
            exit( 1 );
        }

        this->processMembership();

    }else printf("received message of unknown message type %d with %d bytes\n", service_type, ret);

}


void _SpreadManager::processMessage()
{
    //message should contain the format
    //op:[op] id:[id] trans:[trans]

    //op: helo                                            #I'm a thrudb instance
    //op: dbstate last_backup_name redo_size last_redo_id #current backup replication state
    //op: recover                                         #apply redo log
    //op: backup  last_backup_name                        #message sent when a backup occurs

    //op: online   #ready to accept writes
    //op: offline  #not ready to accept writes

    //op: transaction_start  id
    //op: transaction_ok     id
    //op: transaction_fail   id
    //op: transaction_commit id
    //op: transaction_rollback id

    LOG4CXX_DEBUG(logger,"Entering processMessage()");

    //"parse" message
    mparts = split(mess, " ");

    //Invalid message
    if(mparts.size() == 0)
        return;

    string op = mparts[0];

    LOG4CXX_DEBUG(logger,string("Mess: ")+mess);

    //Invalid message
    if( op.substr(0,3) != "op:" )
        return;

    //helo is sent by a thrudb instance on startup
    if( op == "op:helo" )
        return this->processHelo();

    if( op == "op:dbstate" )
        return this->processDbState();

    if( op == "op:transaction_start" )
        return this->processTransactionStart();

    if( op == "op:transaction_ok" )
        return this->processTransactionOk();

    if( op == "op:transaction_fail" )
        return this->processTransactionFail();

    if( op == "op:transaction_commit" )
        return this->processTransactionCommit();

    if( op == "op:transaction_rollback" )
        return this->processTransactionRollback();


    LOG4CXX_ERROR(logger,string("Recieved unknown message: ")+mess);
}

void _SpreadManager::processHelo()
{

    LOG4CXX_DEBUG(logger,"Entering processHelo()");

    //Initialise this instance and set it to offline
    {
        Guard g(mutex);
        members[sender] = OFFLINE;
    }

    //Send a response directly to the sender if this instance
    //Is announcing itself to the group
    if( strcasecmp( reciever, private_group ) != 0 ){
        LOG4CXX_DEBUG(logger,"Responding to sender");

        this->send( sender, "op:helo" );
    }

    LOG4CXX_DEBUG(logger,string("Responding set")+reciever+" "+private_group);
}

void _SpreadManager::processDbState()
{

    LOG4CXX_DEBUG(logger,"Entering processDbState()");

    //op:dbstate last_snapshot_name redo_log_size last_transaction_id
    if( mparts.size() < 5 ){
        LOG4CXX_ERROR( logger, "dbstate::Invalid arg count");
        return;
    }

    DbState remote_state;
    remote_state.last_snapshot_name  = mparts[1];
    remote_state.redo_log_size       = atoi(mparts[2].c_str());
    remote_state.last_timestamp      = atoi(mparts[3].c_str());
    remote_state.last_transaction_id = mparts[4];


    //Send a response directly to the sender if this instance
    //Is announcing itself to the group
    if( strcasecmp( reciever, private_group ) != 0 ){

        LOG4CXX_DEBUG(logger,"Responding to sender");

        DbState state = RecoveryManager->getDbState();
        char buf[128];  sprintf(buf, "%d %ld",state.redo_log_size,state.last_timestamp);

        this->send( sender, (string("op:dbstate ")+state.last_snapshot_name+" "+buf+" "+state.last_transaction_id).c_str() );
    }

    //compare remote state to local state if out of date then update ourselves
    RecoveryManager->compareDbState( sender,remote_state );

    //    member_states[ sender ] = remote_state;
}

void _SpreadManager::processMembership()
{

   if ( Is_reg_memb_mess( service_type ) )
   {

       //reset timestamp data for member who left
       if( Is_caused_leave_mess( service_type ) || Is_caused_disconnect_mess( service_type ) ){
           Guard g(mutex);
           members.erase( memb_info.changed_member );
       }

   }else if( Is_transition_mess(   service_type ) ) {
       printf("received TRANSITIONAL membership for group %s\n", sender );

   }else if( Is_caused_leave_mess( service_type ) ){
       printf("received membership message that left group %s\n", sender );

   }else
       printf("received incorrect membership message of type %d\n", service_type );

}

void _SpreadManager::processTransactionStart()
{
    LOG4CXX_DEBUG(logger,"Entering processTransactionStart()");

    if( mparts.size() < 2 ){
        char buf[64];
        sprintf(buf, "%d", (int)mparts.size());
        LOG4CXX_ERROR( logger, string("transaction_start::Invalid arg count: ")+mparts[0]+buf);
        return;
    }


    boost::shared_ptr<Transaction> t(new Transaction(mparts[1], string(sender)) );


    if((members.size()+1) < quorum_size)
        return this->failTransaction(t);


    //For now we are storing the actual message in cache
    string raw_msg = memd->get(mparts[1]);

    //oops, the cache is missing the transaction (notify owner)
    if(raw_msg.empty())
        return this->failTransaction(t);

    //if no task factory to use then fail
    if(task_factory == boost::shared_ptr<SpreadTaskFactory>()){
        return this->failTransaction(t);
    }

    //Save transaction message
    t->setRawBuffer(raw_msg);

    //Keep safe
    TransactionManager->addTransaction(t);

    //Respond to owner the transaction is ready
    this->okTransaction(t);
}

void _SpreadManager::processTransactionFail()
{
    LOG4CXX_DEBUG(logger,"Entering processTransactionFail()");

    if( mparts.size() < 2 ){
        LOG4CXX_ERROR( logger, "transaction_fail::Invalid arg count");
        return;
    }

    boost::shared_ptr<Transaction> t = TransactionManager->findTransaction(mparts[1]);

    //hmm can't find transaction nothing todo then
    if(t == boost::shared_ptr<Transaction>()){
        LOG4CXX_INFO(logger, string("transaction lookup failed for: ")+mparts[1]);
        return;
    }

    //Notify that its failed (this should be originating instance)
    t->setPrepareStatus(false, sender);
}


void _SpreadManager::processTransactionOk()
{
    LOG4CXX_DEBUG(logger,"Entering processTransactionOk()");

    if( mparts.size() < 2 ){
        LOG4CXX_ERROR( logger, "transaction_ok::Invalid arg count");
        return;
    }

    boost::shared_ptr<Transaction> t = TransactionManager->findTransaction(mparts[1]);

    //hmm can't find transaction nothing todo then
    if(t == boost::shared_ptr<Transaction>()){
        LOG4CXX_INFO(logger, string("transaction lookup failed for: ")+mparts[1]);
        return;
    }

    //Notify blocked origionating thread that all this child is ready
    t->setPrepareStatus(true, sender);
}

void _SpreadManager::processTransactionCommit()
{
    LOG4CXX_DEBUG(logger,"Entering processTransactionCommit()");

    if( mparts.size() < 2 ){
        LOG4CXX_ERROR( logger, "transaction_commit::Invalid arg count");
        return;
    }

    boost::shared_ptr<Transaction> t = TransactionManager->findTransaction(mparts[1]);

    //FIXME: when this happens what should i do...
    //hmm can't find transaction
    if(t == boost::shared_ptr<Transaction>()){
        LOG4CXX_INFO(logger, string("transaction lookup failed for: ")+mparts[1]);
        return;
    }

    if(task_factory == boost::shared_ptr<SpreadTaskFactory>()){
        LOG4CXX_ERROR(logger, string("Missing Task Factory!"));
        exit(-1);
    }

    boost::shared_ptr<Runnable> task = task_factory->defineTask(t);

    shared_ptr<Thread> thread =
        thread_factory->newThread(task);

    thread->start();
}

void _SpreadManager::processTransactionRollback()
{

    LOG4CXX_DEBUG(logger,"Entering processTransactionRollback()");

    if( mparts.size() < 2 ){
        LOG4CXX_ERROR( logger, "transaction_rollback::Invalid arg count");
        return;
    }

    //Just clear it
    TransactionManager->endTransaction(mparts[1]);
}

bool _SpreadManager::startTransaction( boost::shared_ptr<Transaction> t )
{

    std::map<string, int> starting_members;

    {
        Guard g(mutex);
        if( (members.size()+1) < quorum_size ){

            char buf[64];
            sprintf(buf, "%d %d", (int)members.size(), (int)quorum_size);

            LOG4CXX_DEBUG(logger,string("Members less than required for writes: ")+buf );

            return false;
        }

        t->setMembers( members );
    }

    this->send( spread_group.c_str(), (string("op:transaction_start ")+t->getId()).c_str(), true);

    //This will block till trans is ready on all instances
    bool status = t->prepare();

    if(status)
        this->commitTransaction(t);

    return status;
}

void _SpreadManager::failTransaction( boost::shared_ptr<Transaction> t )
{
    this->send((t->getOwner()).c_str(), (string("op:transaction_fail ")+t->getId()).c_str());
}

void _SpreadManager::okTransaction( boost::shared_ptr<Transaction> t )
{
    this->send((t->getOwner()).c_str(), (string("op:transaction_ok ")+t->getId()).c_str());
}

void _SpreadManager::commitTransaction( boost::shared_ptr<Transaction> t )
{
    this->send(spread_group.c_str(), (string("op:transaction_commit ")+t->getId()).c_str(),true);
}

void _SpreadManager::rollbackTransaction( boost::shared_ptr<Transaction> t )
{
    this->send(spread_group.c_str(), (string("op:transaction_rollback ")+t->getId()).c_str(),true);
}

//This is inherently thread-safe thx to libtspread
void _SpreadManager::send( const char *recipient, const char *message, bool ignore_self )
{
    int ret= SP_multicast( fd, (ignore_self ? SAFE_MESS | SELF_DISCARD : SAFE_MESS), recipient, 0, strlen(message), message );

    if( ret < 0 )
    {
        SP_error( ret );
    }
}
