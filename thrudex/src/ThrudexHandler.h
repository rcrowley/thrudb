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
#include "ThrudexBackend.h"

class ThrudexHandler : virtual public thrudex::ThrudexIf
{
    public:
        ThrudexHandler(boost::shared_ptr<ThrudexBackend> backend);
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

        virtual void optimize(const std::string &domain, 
                              const std::string &index);
        virtual void optimizeAll();

        virtual void commit(const std::string &domain, 
                            const std::string &index);
        virtual void commitAll();

    private:
        boost::shared_ptr<ThrudexBackend> backend;
};


#endif

