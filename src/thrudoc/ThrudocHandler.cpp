/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "ThrudocHandler.h"
#include "BloomManager.h"
#include "ConfigFile.h"
#include "utils.h"

#include <uuid/uuid.h>

using namespace std;

ThrudocHandler::ThrudocHandler( boost::shared_ptr<ThrudocBackend> b )
{
    this->backend = b;

    memd                   = boost::shared_ptr<Memcache>( new Memcache() );
    string memd_servers    = ConfigManager->read<string>( "MEMCACHED_SERVERS" );
    vector<string> servers = split( memd_servers, "," );

    for(unsigned int i=0; i<servers.size(); i++){
        memd->addServer( servers[i] );
    }

    this->backend->setCacheHandle( memd );
}


void ThrudocHandler::store(std::string &_return, const string &obj, const string &oldid)
{

    //Skip nulls
    if(obj.empty())
        return;

    //Validate or create doc id
    string     id;
    if( this->isValidID( oldid ) ){
        id = oldid;
    } else {
        id = generateUUID();
    }

    //Write it
    backend->write(obj, id);

    //Cache it
    memd->set(id, obj);

    //bloom it
    if(!BloomManager->exists(id)){
        BloomManager->add( id );
    }

    //done
    _return = id;
}

bool ThrudocHandler::remove(const std::string &id)
{
    //Check for valid
    if( !this->isValidID(id) )
        return false;

    //Check bloom filter
    if( !BloomManager->exists(id) )
        return false;

    //remove from cache
    memd->remove( id );

    //delete
    backend->remove( id );

    return true;
}

void ThrudocHandler::fetch(std::string &_return, const std::string &id)
{

    //Check for valid
    if( !this->isValidID(id) )
        return;


    //Check bloom filter
    if( !BloomManager->exists(id) )
        return;


    //check cache
    string data = memd->get(id);

    //found it, return
    if(!data.empty()){
        _return = data;
        return;
    }

    //read
    data = backend->read(id);

    //set in cache
    memd->set(id,data);

    //yay
    _return = data;
}

void ThrudocHandler::fetchIds(std::vector<std::string> &_return, const int32_t offset, const int32_t limit)
{

    string doc_root = ConfigManager->read<string>("DOC_ROOT");


    string     idx_file = doc_root + "/bloom.idx";
    struct stat idx_stat;
    bool     idx_exists = file_exists( idx_file, idx_stat );


    std::ifstream infile;
    infile.open(idx_file.c_str());
    if (!infile.is_open())
    {
        cerr<<"Error: can't open bloom filter index";
        return;
    }

    int count = 0;

    if( idx_stat.st_size > 0)
        count = (int)(idx_stat.st_size / (UUID_LENGTH+1));


    if(offset > count)
        return;

    //Get to offset
    infile.seekg(offset*(UUID_LENGTH+1),ios::beg);

    int start = (offset+limit) < count ? 0 : offset;
    int end   = (offset+limit) < count ? limit : count;


    for(int i=start; i<end; i++){
        string line;

        getline(infile, line);

        _return.push_back(line);
    }

}


bool ThrudocHandler::removeList(const std::vector<std::string> &ids)
{

    for(unsigned int i=0; i<ids.size(); i++)
        backend->remove(ids[i]);

    return true;
}

void ThrudocHandler::fetchList(std::vector<std::string> &_return, const std::vector<std::string> &ids)
{
    for(unsigned int i=0; i<ids.size(); i++){
        string obj;
        this->fetch( obj, ids[i] );
        _return.push_back( obj );
    }
}

bool ThrudocHandler::isValidID(const std::string &id)
{
    //invalid id
    if(id.length() != UUID_LENGTH)
        return false;

    uuid_t uuid;

    if( uuid_parse(id.c_str(), uuid) != 0 )
        return false;

    return true;
}



