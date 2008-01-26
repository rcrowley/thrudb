/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __CLUCENE_BACKEND__
#define __CLUCENE_BACKEND__

#include "ThrudexBackend.h"

#include <CLucene.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

class CLuceneBackend : public ThrudexBackend
{
    public:
        CLuceneBackend();
        ~CLuceneBackend();

        void    add(const thrudex::DocMsg &d);
        void update(const thrudex::DocMsg &d);
        void remove(const thrudex::RemoveMsg &r);
        thrudex::QueryResponse query(const thrudex::QueryMsg &q);

        virtual void addList(const std::vector<thrudex::DocMsg> &d);
        virtual void updateList(const std::vector<thrudex::DocMsg> &u);
        virtual void removeList(const std::vector<thrudex::RemoveMsg> & d);
        std::vector<thrudex::QueryResponse> queryList
            (const std::vector<thrudex::QueryMsg> &q);

        void ping();

        virtual void optimize(const std::string &domain, 
                              const std::string &index);
        virtual void optimizeAll();

        virtual void commit(const std::string &domain, 
                            const std::string &index);
        virtual void commitAll();

    protected:

        void _addList(const std::vector<thrudex::DocMsg> &d);
        void _updateList(const std::vector<thrudex::DocMsg> &u);
        void _removeList(const std::vector<thrudex::RemoveMsg> & d);

        void _optimize(const std::string &domain, const std::string &index);
        void _optimizeAll();

        void _commit(const std::string &domain, const std::string &index);
        void _commitAll();

        void add(std::string &domain, std::string &index, 
                 std::vector<lucene::document::Document *> &to_add);
        void remove(std::string &domain, std::string &index, 
                    std::vector<wstring> &to_remove);

        std::map<std::string, 
            boost::shared_ptr<lucene::search::IndexSearcher> > indexes;
        std::map<std::string, int64_t> idx_versions;

        lucene::analysis::standard::StandardAnalyzer analyzer;

        boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer>  
            transport;
        boost::shared_ptr<thrudex::ThrudexClient> faux_client;

};

#endif
