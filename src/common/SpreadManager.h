/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __SPREAD_MANAGER__
#define __SPREAD_MANAGER__

#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include <sys/types.h>
#include <sp.h>
#include <event.h>

#include <log4cxx/logger.h>

#include "utils.h"
#include "ConfigFile.h"
#include "RecoveryManager.h"
#include "Transaction.h"
#include "SpreadTaskFactory.h"

#include <vector>
#include <map>
#include <string>

//Singleton class for spread messaging
class _SpreadManager : public facebook::thrift::concurrency::Runnable
{
 public:
    static _SpreadManager* instance();

    void startup();

    void setTaskFactory( boost::shared_ptr<SpreadTaskFactory> stf ){
        this->task_factory = stf;
    }

    //Runs libevent loop
    void run();

    //am i configured to run?
    bool isEnabled() const;

    //For Multi-Master Transaction Processing
    bool startTransaction   ( boost::shared_ptr<Transaction> t );
    void failTransaction    ( boost::shared_ptr<Transaction> t );
    void okTransaction      ( boost::shared_ptr<Transaction> t );
    void commitTransaction  ( boost::shared_ptr<Transaction> t );
    void rollbackTransaction( boost::shared_ptr<Transaction> t );

    const std::string getPrivateName(){
        return private_group;
    };

    const std::map< string, int > getMemberList(){
        facebook::thrift::concurrency::Guard g(mutex);
        return members;
    }

    boost::shared_ptr<SpreadTaskFactory> task_factory;

private:
    _SpreadManager(){};

    class Task;

    facebook::thrift::concurrency::Mutex mutex;      ///< for singleton synch

    static _SpreadManager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;

    bool    spread_enabled;
    string  spread_server;
    string  spread_group;
    string  spread_name;
    size_t  quorum_size;

    //Spread message constructs
    char            private_group[MAX_GROUP_NAME];
    char            mess[102400];
    char            sender[MAX_GROUP_NAME];
    char            *reciever;
    char            target_groups[100][MAX_GROUP_NAME];
    int             num_groups;
    membership_info memb_info;
    int             service_type;

    //Socket
    mailbox fd;

    //
    std::vector<string>  mparts;

    //Stores active member list and status
    std::map< string, int >     members;
    std::map< string, DbState > member_states;

    //Am i online?
    bool is_online;

    //called by libevent handler
    void readMessage();

    //General spread handlers
    void processMessage();
    void processMembership();

    void send(const char *recipient, const char *message, bool ignore_self = false);

    //Specific spread handlers
    void processHelo();
    void processDbState();

    void processTransactionStart();
    void processTransactionOk();
    void processTransactionFail();

    void processTransactionCommit();
    void processTransactionRollback();

    //
    static log4cxx::LoggerPtr logger;

    boost::shared_ptr<facebook::thrift::concurrency::PosixThreadFactory>
        thread_factory;


    // libevent Handler wrapper
    static void eventHandler(int _fd, short which, void* v) {
        ((_SpreadManager*)v)->readMessage();
    }
};


//Singleton shortcut
#define SpreadManager _SpreadManager::instance()

#endif
