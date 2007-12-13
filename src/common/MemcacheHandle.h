#ifndef __MEMCACHE_HANDLE__
#define __MEMCACHE_HANDLE__

#include <pthread.h>
#include "memcache++.h"
#include "ConfigFile.h"
#include "utils.h"

static pthread_key_t memcache_key;

//Uses thread specific storage
class MemcacheHandle
{
public:
   static Memcache *instance()
   {
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
 private:
   static bool init;
};

#endif
