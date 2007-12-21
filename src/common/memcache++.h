#ifndef _MEMCACHE_CPP_H_
#define _MEMCACHE_CPP_H_

/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 *
 * Memcache++ - A Simple c++ wrapper for libmemcached
 *
 *
 **/
#include <iostream>
#include <string>
#include <vector>
#include <libmemcached/memcached.h>

//Exception classes
class MemcacheException{
 public:
 MemcacheException(const char *what) : w(what){};
    std::string w;
};

class MemcacheConnectionException : public MemcacheException{
 public:
 MemcacheConnectionException(const char *what) :
    MemcacheException(what){};
};

class MemcacheSocketException     : public MemcacheException{
 public:
 MemcacheSocketException(const char *what) :
    MemcacheException(what){};
};


class Memcache
{
 public:
    Memcache(){
        memc         = memcached_create(NULL);
        memc_servers = NULL;

        //flags
        memcached_behavior_set (memc,MEMCACHED_BEHAVIOR_NO_BLOCK,0);
        memcached_behavior_set (memc,MEMCACHED_BEHAVIOR_TCP_NODELAY,0);

        memcached_hash hash_value= MEMCACHED_HASH_KETAMA;
        memcached_behavior_set (memc,MEMCACHED_BEHAVIOR_HASH,&hash_value);

        memcached_server_distribution dist_value= MEMCACHED_DISTRIBUTION_CONSISTENT;
        memcached_behavior_set (memc,MEMCACHED_BEHAVIOR_DISTRIBUTION,&dist_value);

    }

    ~Memcache(){
        memcached_server_list_free(memc_servers);
        memcached_free(memc);
    }


    void addServers( std::string servers ){
        memc_servers = memcached_servers_parse ((char *)servers.c_str());

        rc= memcached_server_push(memc, memc_servers);

        if(rc != MEMCACHED_SUCCESS){
            std::cerr<< memcached_strerror(memc,rc) <<std::endl;
            throw MemcacheConnectionException( memcached_strerror(memc,rc) );
        }


        /*bool min_connections = false;

        for(unsigned int i=0; i<memcached_server_list_count(memc_servers); i++){
            rc = memcached_connect(memc,i);

            if(rc == MEMCACHED_SUCCESS)
                min_connections = true;

        }

        if(!min_connections){

            std::cerr<<"No memcached connections available"<<std::endl;
            assert(min_connections == true);

            }*/

    }



    void set(std::string key, const std::string value, const time_t expires = 0) {

        rc = memcached_set(memc, (char *)key.c_str(), key.size(), (char *)value.c_str(), value.size(),expires, (uint16_t)0);

        if(rc != MEMCACHED_SUCCESS){
            std::cerr<<memcached_strerror(memc,rc) <<std::endl;
            throw MemcacheSocketException( memcached_strerror(memc,rc) );
        }
    }


    /*   void add(std::string key, const std::string value, const int expires = 0){

    }

    void replace(std::string key, const std::string value, const int expires = 0);
    */


    std::string get(std::string key){

        size_t sz;
        char *value = memcached_get(memc, (char *)key.c_str(), key.size(), &sz,(uint16_t)0, &rc);

        if( value == NULL )
            return std::string();

        return std::string(value,sz);
    }

    void remove(std::string key, time_t time = 0) {

        rc = memcached_delete(memc, (char *)key.c_str(), key.size(), time);

        if(rc != MEMCACHED_SUCCESS)
            throw MemcacheSocketException( memcached_strerror(memc,rc) );
    }

        /*
void flushAll(unsigned int time = 0);

    int incr(std::string key, const unsigned int value = 1);

    int decr(std::string key, const unsigned int value = 1);
    */

 private:
  memcached_server_st *memc_servers;
  memcached_st        *memc;
  memcached_return    rc;

};

#endif


