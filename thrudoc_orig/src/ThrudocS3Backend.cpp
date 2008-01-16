/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#include "ThrudocS3Backend.h"

#include "Thrudoc.h"
#include "utils.h"
#include "SpreadManager.h"
#include "TransactionManager.h"
#include "MemcacheHandle.h"
#include "s3_glue.h"

#include <fstream>
#include <stdexcept>

using namespace s3;
using namespace std;
using namespace thrudoc;
using namespace log4cxx;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::concurrency;

#define memd MemcacheHandle::instance()

LoggerPtr ThrudocS3Backend::logger(Logger::getLogger("ThrudocS3Backend"));

ThrudocS3Backend::ThrudocS3Backend()
{
    s3bucket  = ConfigManager->read<string>("S3_BUCKET_NAME");

    transport = boost::shared_ptr<TMemoryBuffer>(new TMemoryBuffer());

    boost::shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));

    faux_client = boost::shared_ptr<ThrudocClient>(new ThrudocClient(protocol));
}

string ThrudocS3Backend::read( const string &id )
{
    class response_buffer *b = NULL;
    ThrudocException e;

    b = object_get(s3bucket,id,0);

    if(b == NULL){
        e.what = "HTTP transport error";
        throw e;
    }

    if(b->result == 404) {
        e.what = string("S3: ")+id+" not found";
        throw e;
    }

    string result(b->base,b->len);
    delete b;

    return result;
}


void ThrudocS3Backend::remove(const string &id)
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

void ThrudocS3Backend::_remove(const string &id)
{
    ThrudocException e;

    int r = object_rm(s3bucket,id);

    if(r == -1){
        e.what = "HTTP error";
        throw e;
    }
}

void ThrudocS3Backend::write( const string &doc, const string &id )
{
    LOG4CXX_DEBUG(logger,"Entering S3Backend write()"+id);


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

void ThrudocS3Backend::_write( const string &doc, const string &id)
{
    ThrudocException e;

    struct s3headers meta[2] = {{0,0},{0,0}};

    int r = object_put(s3bucket, id, doc.c_str(), doc.length(),meta);

    if(r == -1){
        e.what = "HTTP error";
        throw e;
    }
}
