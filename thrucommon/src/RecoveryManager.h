/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __RECOVERY_MANAGER__
#define __RECOVERY_MANAGER__

#include <ConfigFile.h>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/transport/TFileTransport.h>

#include <vector>

#include "sp.h"
#include "event.h"

#include "Redo.h"

class DbState
{
 public:
    std::string  last_snapshot_name;
    unsigned int redo_log_size;
    std::string  last_transaction_id;
    time_t       last_timestamp;
};

//Singleton class for spread messaging
class _RecoveryManager : public facebook::thrift::concurrency::Runnable
{
 public:
    enum RecoveryOps {
        SNAPSHOT_RECOVERY,
        TAKE_SNAPSHOT,
        REDO_RECOVERY
    };

    void   startup();
    static _RecoveryManager* instance();

    void run();

    DbState getDbState();

    void    compareDbState(const char *sender, DbState &remote_state);

    void    addRedo(const string &msg, string transaction_id );
    void    addS3( string transaction_id );


    std::string getRedoLogfileName() {
        return redo_log_file;
    }

private:
    _RecoveryManager();

    facebook::thrift::concurrency::Mutex mutex;      ///< for singleton synch

    static _RecoveryManager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;

    bool  isLatestSnapshot();

    const std::string  getLatestSnapshotName();
    //const unsigned int getRedoLogSize();
    //const std::string  getLastTransactionId();

    bool started;


    //Redo Logging
    boost::shared_ptr<facebook::thrift::transport::TFileTransport> redo_log;

    //path and binary locations
    string  thrudb_root;
    string  doc_root;
    string  snapshot_bin;
    string  snapshot_dir;
    string  redo_log_file;

    //msg queue for intra-thread communication
    std::vector< std::pair<RecoveryOps, std::string> > msg_queue;
};

//Singleton shortcut
#define RecoveryManager _RecoveryManager::instance()

#endif
