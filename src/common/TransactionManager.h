/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __TRANSACTION_MANAGER__
#define __TRANSACTION_MANAGER__


#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>

#include "Transaction.h"
#include "ConfigFile.h"

#include <map>

//Singleton class for transaction tracking
class _TransactionManager
{
 public:
    static _TransactionManager* instance();

    //returns transaction
    boost::shared_ptr<Transaction> beginTransaction();
    void                           addTransaction(boost::shared_ptr<Transaction> t);
    boost::shared_ptr<Transaction> findTransaction(string &trans_id);

    void                           endTransaction(boost::shared_ptr<Transaction> t);
    void                           endTransaction(const string &trans_id);

 private:
    _TransactionManager(){};
    facebook::thrift::concurrency::Mutex mutex;      ///< for singleton synch

    //Key is transaction id
    std::map< std::string, boost::shared_ptr<Transaction> >
        trans_lookup;

    std::string trans_id;

    static _TransactionManager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;
};

//Singleton shortcut
#define TransactionManager _TransactionManager::instance()

#endif
