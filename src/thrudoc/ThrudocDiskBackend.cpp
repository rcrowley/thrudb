/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "ThrudocDiskBackend.h"

#include "Thrudoc.h"
#include "utils.h"
#include "SpreadManager.h"
#include "TransactionManager.h"

#include <fstream>
#include <stdexcept>

using namespace std;
using namespace thrudoc;
using namespace log4cxx;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::concurrency;

LoggerPtr ThrudocDiskBackend::logger(Logger::getLogger("ThrudocDiskBackend"));

static Mutex global_mutex = Mutex();

static int safe_mkdir(const char* path, long mode)
{
    Guard g(global_mutex);

    return mkdir(path,mode);
}

static map<string, bool> global_directories = map<string,bool>();

static bool safe_directory_exists( string path )
{
    Guard g(global_mutex);

    if( global_directories.count(path) > 0 ) {
        return true;
    }

    if( directory_exists( path ) ){
        global_directories[path] = true;
        return true;
    }

    return false;
}

ThrudocDiskBackend::ThrudocDiskBackend()
{
    doc_root     = ConfigManager->read<string>("DOC_ROOT");

    transport = boost::shared_ptr<TMemoryBuffer>(new TMemoryBuffer());

    boost::shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));

    faux_client = boost::shared_ptr<ThrudocClient>(new ThrudocClient(protocol));
}

string ThrudocDiskBackend::read( const string &id )
{
    string file = build_filename( id );

    std::ifstream infile;
    infile.open(file.c_str(), ios::in | ios::binary | ios::ate);

    if (!infile.is_open()){
        ThrudocException e;
        e.what = "Error: can't read "+id;
        throw e;
    }

    ifstream::pos_type size = infile.tellg();
    char          *memblock = new char [size];

    infile.seekg (0, ios::beg);
    infile.read (memblock, size);

    infile.close();

    string obj(memblock,size);

    delete [] memblock;

    return obj;
}


void ThrudocDiskBackend::remove(const string &id)
{
    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_remove(id);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_remove(id);

            } else {
                ThrudocException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_remove(id);
        }

    } catch (MemcacheException e) {
        LOG4CXX_ERROR(logger,"Memcached Error");
        throw;

    } catch (ThrudocException e){
        LOG4CXX_ERROR(logger,string("Thrudoc Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX_ERROR(logger,"Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);


    //    RecoveryManager->addRedo( raw_msg );

}

void ThrudocDiskBackend::_remove(const string &id)
{
    string file = build_filename( id );

    if( file_exists( file ) ) {
        if( 0 != unlink( file.c_str() ) ){
            ThrudocException e;
            e.what = "Can't remove "+id;
            throw e;
        }
    }
}

void ThrudocDiskBackend::write( const string &doc, const string &id )
{

    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_store(doc,id);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                  this->_write(doc, id);

            } else {
                ThrudocException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_write(doc, id);
        }

    } catch (MemcacheException e) {
        LOG4CXX_ERROR(logger,"Memcached Error");
        throw;

    } catch (ThrudocException e){
        LOG4CXX_ERROR(logger,string("Thrudoc Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX_ERROR(logger,"Error saving transaction");
        return;
    }

    RecoveryManager->addRedo( raw_msg, t->getId() );

    TransactionManager->endTransaction(t);

}

void ThrudocDiskBackend::_write( const string &doc, const string &id )
{
    string dir_p1 = id.substr(0,2);
    string dir_p2 = id.substr(2,2);
    string dir_p3 = id.substr(4,2);

    if( !safe_directory_exists(string(doc_root+"/"+dir_p1+"/"+dir_p2+"/"+dir_p3)) ){

        if( !safe_directory_exists(doc_root+"/"+dir_p1) )
        {
            if( 0 != safe_mkdir( string(doc_root+"/"+dir_p1).c_str(), 0777L ) ){
                ThrudocException e;
                e.what = "Could not mkdir:"+string(doc_root+"/"+dir_p1);
                throw e;
            }
        }

        if( !safe_directory_exists(doc_root+"/"+dir_p1+"/"+dir_p2) )
        {
            if( 0 != safe_mkdir( string(doc_root+"/"+dir_p1+"/"+dir_p2).c_str(), 0777L ) )
                throw ThrudocException();
        }

        if( 0 != safe_mkdir( string(doc_root+"/"+dir_p1+"/"+dir_p2+"/"+dir_p3).c_str(), 0777L ) )
            throw ThrudocException();
    }

    string file = build_filename( id );

    std::ofstream outfile;
    outfile.open(file.c_str(), ios::out | ios::binary | ios::trunc);

    if (!outfile.is_open()){
        ThrudocException e;
        e.what = "Error: can't write "+id;
        throw e;
    }

    outfile.write(doc.data(), doc.size());

    outfile.close();
}
