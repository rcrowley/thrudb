/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "Transaction.h"
#include "utils.h"

#include "SpreadManager.h"

using namespace facebook::thrift::concurrency;

Transaction::Transaction(std::string _id, std::string o)
    : prepare_complete(false), new_transaction(true), local_transaction(true)
{
    if( _id.empty() ){
        this->id = generateUUID();
    }else{
        this->id        = _id;
        new_transaction = false;
    }

    owner = o;

    if(SpreadManager->isEnabled() && SpreadManager->getPrivateName() != owner){
        local_transaction = false;

        if(owner.empty())
            owner = SpreadManager->getPrivateName();
    }
}

bool Transaction::prepare()
{

    if( !SpreadManager->isEnabled() )
        return true;

    //Wait till spread notifies us
    {
        Synchronized s(m);
        while( !prepare_complete ){
            m.wait();
        }
    }

    //reset
    prepare_complete = false;

    return prepared;
}

void Transaction::setPrepareStatus( bool status, string member )
{
    if( !SpreadManager->isEnabled() )
        return;

    unsigned int num_prepared = 0;

    //This would be normal
    if( status == true && members.count( member ) == 1 ){

        prepared_members[ member ] = status;

        std::map<string,int>::iterator it;
        for(it = members.begin(); it != members.end(); ++it){
            if( prepared_members.count( it->first ) )
                num_prepared++;
        }

    } else {//this would be odd
        status = false;

    }

    //If status has failed then quit here
    //If all nodes are prepared then notify
    if(status == false || num_prepared == members.size())
    {
        Synchronized s(m);
        prepared         = status;
        prepare_complete = true;
        m.notify();  //notifyAll messes up thrift libs
    }
}


void Transaction::commit()
{
   if( !SpreadManager->isEnabled() )
        return;


}

void Transaction::rollback()
{
   if( !SpreadManager->isEnabled() )
        return;



}
