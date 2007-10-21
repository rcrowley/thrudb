/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "BloomManager.h"
#include <stdio.h>
#include <iostream>
#include <stdexcept>
#include <cmath>

#include <transport/TFileTransport.h>
#include <protocol/TBinaryProtocol.h>


#include "ConfigFile.h"
#include "utils.h"


using namespace std;
using namespace boost;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;


_BloomManager* _BloomManager::pInstance = 0;
Mutex          _BloomManager::_mutex    = Mutex();

_BloomManager* _BloomManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _BloomManager();
        }
    }

    return pInstance;
}


void _BloomManager::startup()
{

    string doc_root = ConfigManager->read<string>("DOC_ROOT");

    if( !directory_exists( doc_root ) )
        throw runtime_error("Document root:"+doc_root+" does not exist");

    string     idx_file = doc_root + "/bloom.idx";
    struct stat idx_stat;
    bool     idx_exists = file_exists( idx_file, idx_stat );


    if( !idx_exists ){
        cerr<<"Warning: bloom filter index missing"<<endl;
    }


    filter_space = 10000000;
    filter_size  = 0;

    //Recreate the bloom filter by replaying the log
    if( idx_exists ){
        //always a uuid
        //W4QZtu5rTsWnfyiixxIuFQ\n

        double count = 0;

        if( idx_stat.st_size > 0)
            count = (idx_stat.st_size / (UUID_LENGTH+1));

        //Verify count
        if( count != floor(count) ) {
            cerr<<"Error: bloom filter index looks corrupted"<<endl;
            throw runtime_error("Error: bloom filter index looks corrupted");
        }


        std::ifstream infile;
        infile.open(idx_file.c_str());
        if (!infile.is_open())
        {
            cerr<<"Error: can't open bloom filter index"<<endl;
            throw runtime_error("Error: can't open bloom filter index");
        }

        filter_space += (unsigned int)count;
        filter_size   = (unsigned int)count;

        //Init the filter
        filter = new bloom_filter(filter_space,1.0/(1.0 * filter_space), ((int) 100000*rand()));
        string line;

        unsigned int i = 0;

        while ( !infile.eof() )
        {
            getline (infile,line);

            if(!line.empty()){
                filter->insert(line);
                i++;
            }
        }
        infile.close();

        if( i != filter_size ){
            cerr<<"Warning: didn't catch all the issues:"<<i<<" "<<filter_size<<endl;
        }


    } else {

        //Init the filter
        filter = new bloom_filter(filter_space,1.0/(1.0 * filter_space), ((int) 100000*rand()));
    }

    //Append to the file
    write_file.open(idx_file.c_str(), ios::out | ios::app );
}

void _BloomManager::destroy()
{
    write_file.close();
    delete filter;
}

void _BloomManager::add( const string &id )
{

    if( id.size() != UUID_LENGTH )
        throw runtime_error("BloomManager::add() id: "+id+" is not a valid id");

    Guard g(mutex);
    filter->insert(id);

    write_file << id << endl;
}



bool _BloomManager::exists( const string &id )
{
    if( id.size() != UUID_LENGTH )
        throw runtime_error("BloomManager::exists() id: "+id+" is not a valid id");

    Guard g(mutex);
    return filter->contains(id);
}
