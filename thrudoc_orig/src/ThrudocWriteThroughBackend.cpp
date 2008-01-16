/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "ThrudocWriteThroughBackend.h"

#include "Thrudoc.h"
#include "utils.h"
#include "SpreadManager.h"
#include "TransactionManager.h"
#include "MemcacheHandle.h"

#include <fstream>
#include <stdexcept>

using namespace std;
using namespace thrudoc;
using namespace log4cxx;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::concurrency;

LoggerPtr ThrudocWriteThroughBackend::logger(Logger::getLogger("ThrudocWriteThroughBackend"));

ThrudocWriteThroughBackend::ThrudocWriteThroughBackend()
{

}

string ThrudocWriteThroughBackend::read( const string &id )
{
    //If it was in memcached we wouldn't get here
    //so we can assume memcached lookup failed


    try{
        string disk_doc = disk.read(id);


        if(!disk_doc.empty())
            return disk_doc;

    }catch(ThrudocException e){
        //id does not exist
    }

    string s3_doc  = s3.read(id);

    //if not found then exit
    if( s3_doc.empty() )
        return s3_doc;

    //otherwise store on disk for next time
    disk._write(s3_doc, id);

    return s3_doc;
}

void ThrudocWriteThroughBackend::remove( const string &id )
{

    disk.remove(id);

}


void ThrudocWriteThroughBackend::write( const string &doc, const string &id )
{

    disk.write(doc,id);

}
