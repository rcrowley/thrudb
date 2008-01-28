/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#include "ThruqueueHandler.h"

#include "QueueManager.h"

using namespace boost;
using namespace thruqueue;

void ThruqueueHandler::create(const std::string& queue_name, const bool unique)
{
    QueueManager->createQueue(queue_name, unique);
}

void ThruqueueHandler::destroy(const std::string& queue_name)
{
    QueueManager->destroyQueue(queue_name);
}

void ThruqueueHandler::enqueue(const std::string& queue_name, const std::string& mess, const bool priority)
{
    shared_ptr<Queue> queue = QueueManager->getQueue(queue_name);

    queue->enqueue(mess,priority);
}

void ThruqueueHandler::dequeue(std::string& _return, const std::string& queue_name)
{
    shared_ptr<Queue> queue = QueueManager->getQueue(queue_name);

    _return = queue->dequeue();
}

void ThruqueueHandler::flush(const std::string& queue_name)
{
    shared_ptr<Queue> queue = QueueManager->getQueue(queue_name);

    queue->flush();

}

void ThruqueueHandler::peek(std::string& _return, const std::string& queue_name)
{
    shared_ptr<Queue> queue = QueueManager->getQueue(queue_name);

    _return = queue->peek();
}

int32_t ThruqueueHandler::length(const std::string& queue_name)
{
    shared_ptr<Queue> queue = QueueManager->getQueue(queue_name);

    return queue->length();
}
