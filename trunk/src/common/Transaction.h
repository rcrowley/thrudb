/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#include <thrift/concurrency/Monitor.h>
#include <string>
#include <iostream>

//Not really a transaction but a unit of work...
class Transaction
{
 public:
    Transaction(std::string id = std::string(), std::string owner = std::string());

    const std::string getId()        { return id;         };
    const std::string getOwner()     { return owner;      };
    const std::string &getRawBuffer(){ return raw_buffer; };
    void  setMembers( std::map<std::string, int> members ){
        this->members = members;
    };


    bool prepare          ();
    void setPrepareStatus (bool status, std::string remote_host);
    void commit           ();
    void rollback         ();

    void setRawBuffer( const std::string &raw_msg) {
        raw_buffer = raw_msg;
    };

 private:
    facebook::thrift::concurrency::Monitor m;
    std::string  id;
    std::string  owner;
    std::string  raw_buffer; ///< holds serialized thrift message

    std::map<std::string, int > members;             ///< spread members at time of transaction start
    std::map<std::string, bool> prepared_members;    ///< spread members that have ok'd this trans

    volatile bool prepared;
    volatile bool prepare_complete;

    bool new_transaction;
    bool local_transaction;
};


#endif
