/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __THRUQUEUE_HANDLER_H__
#define __THRUQUEUE_HANDLER_H__

#include "Thruqueue.h"

class ThruqueueHandler : virtual public Thruqueue::ThruqueueIf {
 public:
    ThruqueueHandler() {};

    void ping(){};

    void create(const std::string& queue_name, const bool unique);

    void destroy(const std::string& queue_name);

    void enqueue(const std::string& queue_name, const std::string& mess, const bool priority);

    void dequeue(std::string& _return, const std::string& queue_name);

    void flush(const std::string& queue_name);

    void peek(std::string& _return, const std::string& queue_name);

    int32_t length(const std::string& queue_name);

};

#endif
