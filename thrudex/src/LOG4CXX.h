/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __LUCENE_LOG4CXX__
#define __LUCENE_LOG4CXX__

#include <string>

/**
 *Stupid wrapper class to deal with the fact that
 *clucene and log4cxx don't play nice
 **/

class LOG4CXX
{
 public:
    static void configure( std::string file );

    static void DEBUG( std::string msg );

    static void ERROR( std::string msg );

    static void INFO( std::string msg );

};


#endif
