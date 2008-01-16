/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#include "TransactionManager.h"

using namespace facebook::thrift::concurrency;

_TransactionManager* _TransactionManager::pInstance = 0;
Mutex                _TransactionManager::_mutex    = Mutex();


_TransactionManager* _TransactionManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0)
            pInstance = new _TransactionManager();
    }

    return pInstance;
}

boost::shared_ptr<Transaction> _TransactionManager::beginTransaction()
{
    boost::shared_ptr<Transaction> t(new Transaction());

    string trans_id = t->getId();

    {
        Guard g(mutex);

        //First map it by doc_id then trans_id
        trans_lookup[ trans_id ] = t;
    }

    return t;
}


void _TransactionManager::addTransaction( boost::shared_ptr<Transaction> t )
{

    Guard g(mutex);

    //First map it by doc_id then trans_id
    trans_lookup[ t->getId() ] = t;


}

boost::shared_ptr<Transaction> _TransactionManager::findTransaction( string &trans_id )
{
    {
        Guard g(mutex);
        if( trans_lookup.count( trans_id ))
            return trans_lookup[ trans_id ];
    }

    return boost::shared_ptr<Transaction>();
}

void _TransactionManager::endTransaction( boost::shared_ptr<Transaction> t )
{
    this->endTransaction(t->getId());
}

void _TransactionManager::endTransaction( const string &trans_id )
{
    std::map<string, boost::shared_ptr<Transaction> >::iterator it;

    Guard g(mutex);

    it = trans_lookup.find(trans_id);

    if(it != trans_lookup.end())
        trans_lookup.erase( it );
}
