/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __THRUDOC_HANDLER__
#define __THRUDOC_HANDLER__

#include "Thrudoc.h"
#include "ThrudocBackend.h"

#include <boost/smart_ptr.hpp>
#include <string>
#include <log4cxx/logger.h>


using namespace facebook::thrift;
using namespace thrudoc;

class ThrudocHandler : virtual public ThrudocIf {
 public:
  ThrudocHandler( boost::shared_ptr<ThrudocBackend> b);


  void ping() {};
  void store(std::string &_return, const std::string &obj, const std::string &id);
  bool remove(const std::string &id);
  void fetch(std::string &_return, const std::string &id);

  void fetchIds(std::vector<std::string> &_return, const int32_t offset, int32_t limit);

  bool removeList(const std::vector<std::string> &ids);
  void fetchList(std::vector<std::string> &_return, const std::vector<std::string> &ids);

 private:
  ThrudocHandler(){};

  bool isValidID  (const std::string &id);

  boost::shared_ptr<ThrudocBackend> backend;


  std::string genNewID();


  static log4cxx::LoggerPtr logger;
};


#endif
