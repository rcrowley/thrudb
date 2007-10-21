/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDOC_S3_SPREAD_TASK__
#define __THRUDOC_S3_SPREAD_TASK__

#include "SpreadTaskFactory.h"
#include "Transaction.h"

#include <thrift/concurrency/Thread.h>

#include <boost/shared_ptr.hpp>

class ThrudocS3SpreadTask : public facebook::thrift::concurrency::Runnable {
public:
    ThrudocS3SpreadTask(boost::shared_ptr<Transaction> t);

    void run();

private:
    boost::shared_ptr<Transaction> transaction;
};


class ThrudocS3SpreadTaskFactory : public SpreadTaskFactory
{
 public:
    boost::shared_ptr<facebook::thrift::concurrency::Runnable> defineTask( boost::shared_ptr<Transaction> t ) {
        boost::shared_ptr<facebook::thrift::concurrency::Runnable> task(new ThrudocS3SpreadTask(t));

        return task;
    }
};

#endif
