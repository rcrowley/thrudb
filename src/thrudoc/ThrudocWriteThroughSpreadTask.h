/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDOC_WRITE_THROUGH_SPREAD_TASK__
#define __THRUDOC_WRITE_THROUGH_SPREAD_TASK__

#include "SpreadTaskFactory.h"
#include "Transaction.h"

#include <thrift/concurrency/Thread.h>

#include <boost/shared_ptr.hpp>

class ThrudocWriteThroughSpreadTask : public facebook::thrift::concurrency::Runnable {
public:
    ThrudocWriteThroughSpreadTask(boost::shared_ptr<Transaction> t);

    void run();

private:
    boost::shared_ptr<Transaction> transaction;
};


class ThrudocWriteThroughSpreadTaskFactory : public SpreadTaskFactory
{
 public:
    boost::shared_ptr<facebook::thrift::concurrency::Runnable> defineTask( boost::shared_ptr<Transaction> t ) {
        boost::shared_ptr<facebook::thrift::concurrency::Runnable> task(new ThrudocWriteThroughSpreadTask(t));

        return task;
    }
};

#endif
