/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDEX_HANDLER__
#define __THRUDEX_HANDLER__

#include "Thrudex.h"

#include "CLucene.h"
#include "CLucene/util/Reader.h"
#include <map>

#include <boost/shared_ptr.hpp>

#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

using namespace lucene::index;
using namespace lucene::store;
using namespace lucene::search;
using namespace lucene::analysis;
using namespace lucene::util;
using namespace lucene::queryParser;
using namespace lucene::document;


class ThrudexHandler : virtual public thrudex::ThrudexIf
{
 public:
    ThrudexHandler();
    virtual ~ThrudexHandler();

    void    add(const thrudex::DocMsg &d);
    void update(const thrudex::DocMsg &d);
    void remove(const thrudex::RemoveMsg &r);
    void query(thrudex::QueryResponse &_return, const thrudex::QueryMsg &q);

    virtual void addList(const std::vector<thrudex::DocMsg> &d);
    virtual void updateList(const std::vector<thrudex::DocMsg> &u);
    virtual void removeList(const std::vector<thrudex::RemoveMsg> & d);
    void queryList(std::vector<thrudex::QueryResponse> &_return, const std::vector<thrudex::QueryMsg> &q);

    void ping();

    virtual void optimize(const string &domain, const string &index);
    virtual void optimizeAll();

    virtual void commit(const string &domain, const string &index);
    virtual void commitAll();

 protected:

    void _addList(const std::vector<thrudex::DocMsg> &d);
    void _updateList(const std::vector<thrudex::DocMsg> &u);
    void _removeList(const std::vector<thrudex::RemoveMsg> & d);

    void _optimize(const string &domain, const string &index);
    void _optimizeAll();

    void _commit(const string &domain, const string &index);
    void _commitAll();

    void add(string &domain, string &index, vector<lucene::document::Document *> &to_add);
    void remove(string &domain, string &index, vector<wstring>   &to_remove);

    std::map<string, boost::shared_ptr<IndexSearcher> > indexes;
    std::map<string, int64_t>         idx_versions;

    standard::StandardAnalyzer        analyzer;


    boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer>  transport;
    boost::shared_ptr<thrudex::ThrudexClient>                      faux_client;

};


#endif

