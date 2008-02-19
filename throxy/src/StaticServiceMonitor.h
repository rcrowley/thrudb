#ifndef __STATIC_SERVICE_MONITOR__H__
#define __STATIC_SERVICE_MONITOR__H__

#include "ServiceMonitor.h"
#include "ConfigFile.h"
#include <log4cxx/logger.h>

//Simplest case where you have a config file
//that has a static layout of your services
class StaticServiceMonitor : public ServiceMonitor
{
 public:
    StaticServiceMonitor(const std::string config_file);
    ~StaticServiceMonitor();

 private:
    StaticServiceMonitor();

    std::string config_file;
    ConfigFile  config;

    static log4cxx::LoggerPtr logger;
};

#endif
