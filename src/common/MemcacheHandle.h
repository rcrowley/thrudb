#ifndef __MEMCACHE_HANDLE__
#define __MEMCACHE_HANDLE__

#include <pthread.h>
#include "memcache++.h"
#include "ConfigFile.h"
#include "utils.h"
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>


static pthread_key_t memcache_key;
static bool          init = false;
static facebook::thrift::concurrency::Mutex _mutex = facebook::thrift::concurrency::Mutex();



//Uses thread specific storage
class MemcacheHandle
{
public:
   static Memcache *instance()
   {

       if(!init){
           facebook::thrift::concurrency::Guard g(_mutex);
           if(!init){
               pthread_key_create(&memcache_key, NULL);
               init = true;
           }
       }


       void *ptr = pthread_getspecific(memcache_key);

       if (ptr == NULL)
       {
           Memcache *m = new Memcache();

           std::string memd_servers    = ConfigManager->read<string>( "MEMCACHED_SERVERS" );

           m->addServers( memd_servers );

           ptr = m;
           pthread_setspecific(memcache_key, m);
      }

       return (Memcache *)ptr;
   }

};

#endif
