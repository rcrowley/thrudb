/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "ThrudexHandler.h"

using namespace std;
using namespace thrudex;

ThrudexHandler::ThrudexHandler(boost::shared_ptr<ThrudexBackend> backend)
{
    this->backend = backend;
}


ThrudexHandler::~ThrudexHandler()
{
}

void ThrudexHandler::ping()
{
    this->backend->ping ();
}

void ThrudexHandler::add(const DocMsg &d)
{
    this->backend->add (d);
}

void ThrudexHandler::update(const DocMsg &d)
{
    this->backend->update (d);
}

void ThrudexHandler::remove(const RemoveMsg &r)
{
    this->backend->remove (r);
}

void ThrudexHandler::query(QueryResponse &_return, const QueryMsg &q)
{
    _return = this->backend->query (q);
}

void ThrudexHandler::addList( const vector<DocMsg> &dv )
{
    this->backend->addList (dv);
}

void ThrudexHandler::removeList( const vector<RemoveMsg> &rl )
{
    this->backend->removeList (rl);
}

void ThrudexHandler::updateList( const vector<DocMsg> &ul)
{
    this->backend->updateList (ul);
}

void ThrudexHandler::queryList(vector<QueryResponse> &_return, const vector<QueryMsg> &q)
{
    _return = this->backend->queryList (q);
}

void ThrudexHandler::optimize(const string &domain, const string &index)
{
    this->backend->optimize (domain, index);
}

void ThrudexHandler::optimizeAll()
{
    this->backend->optimizeAll ();
}

void ThrudexHandler::commit(const string &domain, const string &index)
{
    this->backend->commit (domain, index);
}

void ThrudexHandler::commitAll()
{
    this->backend->commitAll ();
}
