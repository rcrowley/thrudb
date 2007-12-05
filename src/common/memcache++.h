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
 * Memcache++ - A C++ client for memcached
 *
 * Derived from memcachedpp by Sergey Prikhodko
 *
 **/
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <thrift/concurrency/Mutex.h>

//Return codes
#define MEMCACHE_STORED          0
#define MEMCACHE_DELETED         0
#define MEMCACHE_SUCCESS         0
#define MEMCACHE_NOT_STORED     -1
#define MEMCACHE_NOT_FOUND      -2
#define MEMCACHE_ERROR          -3

//Exception classes
class MemcacheException{};
class MemcacheConnectionException : public MemcacheException{};
class MemcacheSocketException     : public MemcacheException{};
class MemcacheProtocolException   : public MemcacheException{};
class MemcacheRehashException     : public MemcacheException{};
class MemcacheInternalException   : public MemcacheException{};


class Memcache
{
 public:
  Memcache();
  ~Memcache();

  void addServers (std::map<std::string, unsigned int> &server_and_weights);
  void addServers (const std::vector<std::string> &servers);
  void addServer  (const std::string server, const unsigned int weight = 1);

  int set(std::string key, const std::string value, const int expires = 0);
  int add(std::string key, const std::string value, const int expires = 0);
  int replace(std::string key, const std::string value, const int expires = 0);
  std::string get(std::string key);
  int remove(std::string key, unsigned int time = 0);

  void flushAll(unsigned int time = 0);

  int incr(std::string key, const unsigned int value = 1);
  int decr(std::string key, const unsigned int value = 1);

  std::map< std::string, std::map<std::string,std::string> >  getStats();

  void setCompressionOption(int minsize,int use_compression);
  void setRehashingOption( bool on_off );

 private:

  //Shared code
  int _set(std::string cmd_type,  std::string &key, const std::string &value, const int expires);
  int _incr(const std::string cmd_type, std::string &key, const unsigned int value = 1);

  //hostname, sockfd
  std::map<std::string, int> servers;
  std::vector< std::string > server_alloc_buckets;

  int _connect(const std::string &server);
  int disconnect(const std::string &server);

  /*C style socket calls */
  std::string      _sock_gets(int sockfd, unsigned int limit = ~0);
  int              _sock_puts(int sockfd, std::string str);
  int              _sock_write(int sockfd,const char* buf, size_t count);
  std::string      _sock_read(int sockfd, size_t count);
  int              _comp(const char* s_buf,int s_len,char** d_buf,int& d_len);
  int              _decomp(const char* s_buf,int s_len,char** d_buf,int& d_len);
  struct in_addr*  _atoaddr(std::string& address);

  int              _crc32(const std::string &key);
  std::string      _inttostring(int val);
  int              _get_sock(const std::string &key );


  //Settings and flags
  unsigned int  opt_minsize;
  bool opt_use_compression;
  bool opt_rehash;

  facebook::thrift::concurrency::Mutex m;
};

#endif


