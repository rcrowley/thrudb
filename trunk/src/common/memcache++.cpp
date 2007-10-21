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


#include "memcache++.h"
#include "crc32_table.h"
#include <stdio.h>
#include <zlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>

#include <fcntl.h>
#include <stdio.h>

extern int errno;

using namespace std;

#define USE_COMPRESSION 1


/**************************************************************************/

Memcache::Memcache(void) :
  opt_minsize(10000),
  opt_use_compression(false),
  opt_rehash(false)
{

}

/**************************************************************************/

Memcache::~Memcache()
{
    map<string,int>::iterator it;

    for(it=servers.begin(); it!=servers.end(); ++it)
        this->disconnect(it->first);
}

/**************************************************************************/

void Memcache::addServer(const string server, unsigned int weight)
{

  if( servers.count(server) == 0){
    servers[server] = -1;

    if( weight < 1 ) weight = 1;

    for( unsigned int i=0; i<weight; i++)
        server_alloc_buckets.push_back( server );
  }
}

/**************************************************************************/

void Memcache::addServers(const vector<string> &servers)
{

  for(unsigned int i=0; i<servers.size(); i++) {

    if( this->servers.count(servers[i]) == 0){
      this->servers[servers[i]] = -1;
      server_alloc_buckets.push_back( servers[i] );
    }
  }

}

/**************************************************************************/

void Memcache::addServers( map<string, unsigned int> &servers_and_weights )
{

  map<string, unsigned int>::iterator it;

  for( it=servers_and_weights.begin(); it!=servers_and_weights.end(); ++it ){
    string       server = it->first;
    unsigned int weight = it->second == 0 ? 1 : it->second;

    if( !servers.count( server ) ){
      //to apply weighting across servers we represent weights as entries in a allocation list
      //This list is used to split between servers, so the more entries (or higher weight) of a particular
      //server yields more usage

      for(unsigned int i=0; i<weight; i++){
        server_alloc_buckets.push_back( server );
      }
    }
  }

}

/**************************************************************************/

void Memcache::setCompressionOption(int minsize,int use_compression)
{
    opt_minsize         = minsize;
    opt_use_compression = use_compression;
}

/**************************************************************************/

int Memcache::_get_sock( const string &key )
{

    string server;

    if( servers.empty() )
        throw MemcacheConnectionException();

    //No hashing required
    if( servers.size() == 1 ) {

        server = servers.begin()->first;

        //Yay
        if( servers[server] > 0 )
            return servers[server];

        servers[server] = this->_connect(server);

        return servers[server];
    }


    //Hashing needed
    int tries = 0;

    while( tries < 20 && !server.empty() ){
        int crc = this->_crc32( key + _inttostring(tries) );

        server = server_alloc_buckets[ crc % server_alloc_buckets.size() ];

        //Yay
        if( servers[server] > 0 )
            return servers[server];

        servers[server] = this->_connect(server);

        //Yay
        if( servers[server] > 0 )
            return servers[server];

        //dead socket (err if rehash is false)
        if( servers[server] == -1 && !opt_rehash )
            throw MemcacheRehashException();

        //try again
        tries++;
    }


    //failed to get a active socket
    throw MemcacheConnectionException();
    return -1;
}

/**************************************************************************/

int Memcache::_connect(const string &server)
{
  string host;
  int port;
  int pos;

  if( (pos = server.find(":", 0)) != -1 ) {
      host = server.substr(0, pos);
      port = atoi(server.substr(pos+1, server.length() - pos + 1).c_str());
  } else {
      port = 11211;
      host = server;
  }

  int sock = socket(AF_INET, SOCK_STREAM, getprotobyname("tcp")->p_proto);


  struct in_addr *addr;
  addr = _atoaddr(host);
  struct sockaddr_in saddr;

  memset((char *) &saddr, 0, sizeof(saddr));
  saddr.sin_family = AF_INET;
  saddr.sin_port = htons(port);
  saddr.sin_addr.s_addr = addr->s_addr;


  int rc = connect(sock, (struct sockaddr *) &saddr, sizeof(saddr));

  if (fcntl(sock, F_SETFL, O_NDELAY) == -1)
  {
      close(sock);
      throw MemcacheConnectionException();
  }


  if( rc != 0 )
      throw MemcacheConnectionException();

  return sock;
}

/**************************************************************************/

int Memcache::disconnect(const string &server)
{

    if( servers.count(server) == 0 ){
        return -1;
    }

    int sock = servers[server];

    if (sock > 0)
        close(sock);

    servers[server] = -1;

    return 1;
}

/**************************************************************************/

map< string, map<string,string> > Memcache::getStats()
{

    map< string, map<string,string> > stats;
    map<string,int>::iterator         it;


    for( it = servers.begin(); it != servers.end(); ++it){

        int sock = it->second;

        if( sock < 1) continue;

        _sock_puts(sock, "stats\r\n");

        string line;
        unsigned int pos;

        line = _sock_gets(sock);
        pos = line.find(" ", 5);

        if( pos != string::npos ){
            stats[it->first][ line.substr(5, pos - 5) ] = line.substr( pos + 1, line.length() - pos );
        }

    }

    return stats;
}

/**************************************************************************/

int Memcache::set(string key, const string value, const int expires)
{
    return this->_set("set", key, value, expires);
}

/**************************************************************************/

int Memcache::add(string key, const string value, const int expires)
{
    return this->_set("add", key, value, expires);
}

/**************************************************************************/

int Memcache::replace(string key, const string value, const int expires)
{
    return this->_set("replace", key, value, expires);
}

/**************************************************************************/

int Memcache::_set(string cmd_type, string &key, const string &value, const int expires)
{

    //Make sure the key has no spaces
    unsigned int pos=0;
    while( (pos = key.find(" ",pos) ) != string::npos ){
        key[pos] = '_';
    }

    int sock = this->_get_sock( key );

    if(sock < 0)
        throw MemcacheConnectionException();


    char* tmp_buf     = NULL;
    int   tmp_len     = 0;
    int   is_compress = 0;
    string val        = value;

    if (opt_use_compression && USE_COMPRESSION && value.size() > opt_minsize) {

        if (!_comp(value.c_str(),value.length(),&tmp_buf,tmp_len)){
            val         = string(tmp_buf,tmp_len);

            is_compress = 1;
        }
    }

    string cmd = cmd_type + " " + key + (is_compress?" 1 ":" 0 ") + _inttostring(expires) + " " + _inttostring(val.length()) + "\r\n";

    _sock_write(sock, (cmd+val+"\r\n").data(), cmd.length()+val.length()+2);

    string res = _sock_gets(sock);

    if( res.compare("STORED\r\n") == 0)
        return MEMCACHE_STORED;


    cerr<<"\n|"<<res<<"#\n";
    return MEMCACHE_NOT_STORED;
}


/**************************************************************************/

int Memcache::remove( string key, unsigned int time )
{
    //Make sure the key has no spaces
    unsigned int pos;
    while( (pos = key.find(" ") ) != string::npos ){
        key[pos] = '_';
    }

    int sock = this->_get_sock( key );

    if(sock < 0)
        throw MemcacheConnectionException();

    string cmd = "delete "+ key + " " + _inttostring(time) + "\r\n";

    _sock_write(sock, cmd.c_str(), cmd.length());

    string res = _sock_gets(sock);

    if( res.compare("DELETED\r\n") == 0 )
        return MEMCACHE_DELETED;

    return MEMCACHE_NOT_FOUND;
}

/**************************************************************************/
void Memcache::flushAll(unsigned int time)
{
    map<string,int>::iterator it;

    string cmd = "flush_all\r\n";

    for(it=servers.begin(); it!=servers.end(); ++it){
        int sock = it->second;

        if( sock > 0 ){
            _sock_write(sock, cmd.c_str(), cmd.length());

            string res = _sock_gets(sock);
        }
    }
}

/**************************************************************************/

int Memcache::incr( string key, const unsigned int value )
{
    return this->_incr("incr",key,value);
}

/**************************************************************************/

int Memcache::decr( string key, const unsigned int value )
{
    return this->_incr("decr",key,value);
}

/**************************************************************************/
int Memcache::_incr( const std::string cmd_type, std::string &key, const unsigned int value )
{
    //Make sure the key has no spaces
    unsigned int pos;
    while( (pos = key.find(" ") ) != string::npos ){
        key[pos] = '_';
    }

    int sock = this->_get_sock( key );

    if(sock < 0)
        throw MemcacheConnectionException();

    string cmd = cmd_type+" "+key+" "+ _inttostring(value) + "\r\n";

    _sock_write(sock, cmd.c_str(), cmd.length());

    string res = _sock_gets(sock);

    if( res.compare("NOT_FOUND\r\n") == 0)
        return MEMCACHE_NOT_FOUND;

    return atoi(res.c_str());
}

/**************************************************************************/

string Memcache::get(string key)
{

  string res, r_key, r_flags;
  unsigned int r_bytes, pos, pos2,pos3;

  //Make sure the key has no spaces
  unsigned int spos = 0;
  while( (spos = key.find(" ",spos) ) != string::npos ){
      key[spos] = '_';
  }

  int sock = this->_get_sock( key );

  if(sock < 0)
      throw MemcacheConnectionException();

  _sock_puts(sock, "get " + key + "\r\n");

  res = _sock_gets(sock);

  //Check for missing value
  if( res.compare("END\r\n") == 0)
      return "";

  if ( res.substr(0, 5).compare("VALUE") != 0 )
      throw MemcacheProtocolException();

  pos = res.find( " ", 6);
  if( pos == string::npos )
      throw MemcacheProtocolException();

  r_key = res.substr(6, pos - 6);
  if ( r_key.compare(key) != 0 )
      throw MemcacheProtocolException();

  pos2 = res.find(" ", pos+1);
  if( pos2 == string::npos )
      throw MemcacheProtocolException();

  r_flags = res.substr(pos + 1, pos2 - pos);

  pos3 = res.find("\r\n", pos2+1);
  if( pos3 == string::npos)
      throw MemcacheProtocolException();

  r_bytes = atoi(res.substr(pos2, pos3).c_str());


  string str = res.substr(pos3+2);

  //Get next line
  while(str.size() < r_bytes){
      str += _sock_gets(sock);
  }

  if( str.size() > 4 && str.substr(str.size()-5) != "END\r\n" ){
    str += _sock_gets(sock);

    if( str.size() > 4 && str.substr(str.size()-5) != "END\r\n" ){
        //cerr<<"Didn't FIND END!\n";
    }
  }

  //Cut off END
  str = str.substr(0,r_bytes);


  char* tmp_buf = NULL;
  int   tmp_len = 0;

  if (USE_COMPRESSION && atoi(r_flags.c_str())){

      if (!_decomp(str.data(),r_bytes,&tmp_buf,tmp_len)){
          str = string(tmp_buf,tmp_len);
      } else {
          throw MemcacheInternalException();
      }
  }

  return str;
}

/**************************************************************************/

string Memcache::_sock_gets(int sockfd, unsigned int limit)
{
  string str("");
  char buf[BUFSIZ+1];
  int this_read;
  int total_read = 0;

  facebook::thrift::concurrency::Guard g(m);

  memset(buf,0,BUFSIZ+1);

  while ( str.size() < limit ) {

      do {

          //Means it wrote twice before reading on this socket.
          this_read = read(sockfd, buf, BUFSIZ);

      } while ( (this_read < 0) && (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN ) );

      buf[ this_read ] = '\0';

      total_read += this_read;

      string pstr(buf,this_read);

      //if (pstr.empty())
      //    return "";

      str += pstr;

      if( str.size() >= 2 && str.substr(str.size()-2) == "\r\n" )
          return str;
  }

  exit(-1);
  return str;
}

/**************************************************************************/

int Memcache::_sock_puts(int sockfd, string str)
{

  return _sock_write(sockfd, str.data(), str.length());

}

/**************************************************************************/

int Memcache::_sock_write(int sockfd, const char* buf, size_t count)
{
  size_t bytes_sent = 0;
  int this_write;

  facebook::thrift::concurrency::Guard g(m);

  while (bytes_sent < count) {
    do {
      this_write = write(sockfd, buf, count - bytes_sent);
    } while ( (this_write < 0) && (errno == EINTR ||  errno == EWOULDBLOCK || errno == EAGAIN) );

    if (this_write <= 0)
        throw MemcacheSocketException();

    bytes_sent += this_write;
    buf += this_write;
  }

  return count;
}

/**************************************************************************/

string Memcache::_sock_read(int sockfd, size_t count)
{

    facebook::thrift::concurrency::Guard g(m);

    char buf[BUFSIZ+1];
    size_t bytes_read = 0;
    int this_read;

    memset(buf,0,BUFSIZ+1);

    string res;

    while (bytes_read < count) {

        do {

            this_read = read(sockfd, buf, count - bytes_read);

        } while ( (this_read < 0) && (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) );

        if (this_read <= 0)
            throw MemcacheSocketException();

        buf[ this_read ] = '\0';

        bytes_read += this_read;
        res        += string(buf,this_read);
    }

    return res;
}

/**************************************************************************/

struct in_addr* Memcache::_atoaddr(string& address)
{
    struct hostent        *host;
    static struct in_addr  saddr;

    saddr.s_addr = inet_addr(address.c_str());

    if (saddr.s_addr != 0)
        return &saddr;

    host = gethostbyname(address.c_str());

    if (host != NULL)
        return (struct in_addr *) *host->h_addr_list;

    return NULL;
}

/**************************************************************************/

string Memcache::_inttostring(int val)
{
    char buf[50];

    string str;
    sprintf(buf,"%d",val);
    str=buf;

    return str;
}

/**************************************************************************/

int Memcache::_comp(const char* s_buf,int s_len,char** d_buf,int& d_len)
{
    *d_buf = NULL;
    d_len  = 0;

    if (s_len<=0)
        return 0;

    d_len  = s_len+s_len/10+12;
    *d_buf = (char*)malloc(d_len);

    int st=compress((Bytef*)*d_buf,(uLongf*)&d_len,(Bytef*)s_buf,s_len);

    while ( d_len && st==Z_BUF_ERROR ) {
        d_len *= 10;

        *d_buf = (char*)realloc(*d_buf,d_len);
        st     = compress((Bytef*)*d_buf,(uLongf*)&d_len,(Bytef*)s_buf,s_len);
    }

    return st == Z_OK ? 0:1;
}

/**************************************************************************/

int Memcache::_decomp(const char* s_buf,int s_len,char** d_buf,int& d_len)
{
    *d_buf = NULL;
    d_len  = 0;

    if (s_len <= 0)
        return 0;

    d_len  = 2*s_len;
    *d_buf = (char*)malloc(d_len);

    int st = uncompress((Bytef*)*d_buf,(uLongf*)&d_len,(Bytef*)s_buf,s_len);

    while( d_len && st==Z_BUF_ERROR ){
        d_len *= 10;

        *d_buf = (char*)realloc(*d_buf,d_len);
        st     = uncompress((Bytef*)*d_buf,(uLongf*)&d_len,(Bytef*)s_buf,s_len);
    }


    return st == Z_OK ? 0:1;
}

/**************************************************************************/

int Memcache::_crc32( const string &key )
{

  int crc = ~0;

  for (unsigned int i = 0; i < key.length(); i++)
      crc = (crc >> 8) ^ crc32tab[(crc ^ (key[i])) & 0xff];

  crc = (~crc >> 16) & 0x7fff;

  return crc == 0 ? 1 : crc;
}

