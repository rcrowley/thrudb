/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __THRUDEX_BACKEND__
#define __THRUDEX_BACKEND__

#include "Thrudex.h"

class ThrudexBackend
{
    public:
        virtual ~ThrudexBackend() {};

        virtual void    add(const thrudex::DocMsg &d) = 0;
        virtual void update(const thrudex::DocMsg &d) = 0;
        virtual void remove(const thrudex::RemoveMsg &r) = 0;
        virtual thrudex::QueryResponse query(const thrudex::QueryMsg &q) = 0;

        virtual void addList(const std::vector<thrudex::DocMsg> &d) = 0;
        virtual void updateList(const std::vector<thrudex::DocMsg> &u) = 0;
        virtual void removeList(const std::vector<thrudex::RemoveMsg> & d) = 0;
        virtual std::vector<thrudex::QueryResponse> queryList
            (const std::vector<thrudex::QueryMsg> &q) = 0;

        virtual void ping() = 0;

        virtual void optimize(const std::string &domain, 
                              const std::string &index) = 0;
        virtual void optimizeAll() = 0;

        virtual void commit(const std::string &domain, 
                            const std::string &index) = 0;
        virtual void commitAll() = 0;
};

#endif
