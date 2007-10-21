/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "LOG4CXX.h"


#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

using namespace log4cxx;
using namespace log4cxx::helpers;


static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger(log4cxx::String("ThruceneHandler")));


void LOG4CXX::configure( std::string file){

    //Init logger
    PropertyConfigurator::configure(file);



}

void LOG4CXX::DEBUG(std::string msg)
{
    LOG4CXX_DEBUG(logger,msg);
}

void LOG4CXX::ERROR(std::string msg)
{
    LOG4CXX_ERROR(logger,msg);
}

void LOG4CXX::INFO(std::string msg)
{
    LOG4CXX_INFO(logger,msg);
}
