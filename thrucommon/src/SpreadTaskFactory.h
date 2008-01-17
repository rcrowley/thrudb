/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __SPREAD_TASK_FACTORY_H__
#define __SPREAD_TASK_FACTORY_H__

#include <boost/shared_ptr.hpp>
#include <concurrency/Thread.h>
#include <Transaction.h>

class SpreadTaskFactory
{
 public:
    SpreadTaskFactory(){};
    virtual ~SpreadTaskFactory(){};

    virtual boost::shared_ptr<facebook::thrift::concurrency::Runnable>
        defineTask(boost::shared_ptr<Transaction> t) = 0;
};

#endif
