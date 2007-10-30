/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __BLOOM_MANAGER__
#define __BLOOM_MANAGER__

#include <string>
#include <iostream>
#include <fstream>
#include <boost/shared_ptr.hpp>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>

class bloom_filter;

#define UUID_LENGTH 36
//Singleton class for bloom filter
//existance check
class _BloomManager
{
 public:

    //Replay log creating bloom filter
    //Logging as we go.
    void   startup();
    void   destroy();

    void   add( const std::string &id );

    bool   exists( const std::string &id );

    static _BloomManager* instance();

private:

    _BloomManager(){};

    facebook::thrift::concurrency::Mutex mutex;          ///< for singleton synch


    bloom_filter *filter;
    unsigned int filter_size;
    unsigned int filter_space;

    std::ofstream write_file;


    static _BloomManager* pInstance;
    static facebook::thrift::concurrency::Mutex _mutex;
};


//Singleton shortcut
#define BloomManager _BloomManager::instance()


#endif
