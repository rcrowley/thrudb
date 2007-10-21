/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUCENE_SIMPLE_HANDLER__
#define __THRUCENE_SIMPLE_HANDLER__

#include "ThruceneHandler.h"

//Thrucene handler minus logic used to realise remote transactions

class ThruceneSimpleHandler : public ThruceneHandler
{
 public:

    void addList(const std::vector<DocMsg> &d){
        this->_addList(d);
    };

    void updateList(const std::vector<DocMsg> &u){
        this->_updateList(u);
    };

    void removeList(const std::vector<RemoveMsg> &d){
        this->_removeList(d);
    };

    void optimize(const string &domain, const string &index){
        this->_optimize(domain,index);
    };

    void optimizeAll(){
        this->_optimizeAll();
    };

    void commit(const string &domain, const string &index){
        this->_commit(domain,index);
    };

    void commitAll(){
        this->_commitAll();
    };

};

#endif
