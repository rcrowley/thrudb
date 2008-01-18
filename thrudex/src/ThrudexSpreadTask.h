/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDEX_SPREAD_TASK__
#define __THRUDEX_SPREAD_TASK__

#include "SpreadTaskFactory.h"
#include "Transaction.h"

#include <thrift/concurrency/Thread.h>

#include <boost/shared_ptr.hpp>

class ThrudexSpreadTask : public facebook::thrift::concurrency::Runnable {
public:
    ThrudexSpreadTask(boost::shared_ptr<Transaction> t);

    void run();

private:
    boost::shared_ptr<Transaction> transaction;
};


class ThrudexSpreadTaskFactory : public SpreadTaskFactory
{
 public:
    boost::shared_ptr<facebook::thrift::concurrency::Runnable> defineTask( boost::shared_ptr<Transaction> t ) {
        boost::shared_ptr<facebook::thrift::concurrency::Runnable> task(new ThrudexSpreadTask(t));

        return task;
    }
};

#endif
