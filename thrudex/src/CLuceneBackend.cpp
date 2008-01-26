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

#include "CLuceneBackend.h"

#include "LuceneManager.h"
#include "MemcacheHandle.h"

#include <SpreadManager.h>
#include <TransactionManager.h>
#include <utils.h>

#include "LOG4CXX.h"
#include <cstdlib>
#include <string>

using namespace std;
using namespace thrudex;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::concurrency;

#define memd MemcacheHandle::instance()

inline wstring build_wstring( const string &str )
{
    wstring wtmp;

    wchar_t    tmp_wchar;
    int        nchar;
    mbstate_t  ps;
    const char *c_str = str.c_str();
    size_t size       = str.size();

    memset(&ps, 0, sizeof(ps));
    wtmp = L"";

    while (c_str && size) {
        nchar = mbrtowc(&tmp_wchar, c_str, size, &ps);

        if (nchar == -1) {
            c_str++;
            size--;
            continue;
        }

        //signals end of str
        if (nchar == 0)
            break;

        wtmp    += tmp_wchar;
        c_str   += nchar;
        size    -= nchar;
    }

    return wtmp;
}

inline void wtrim(wstring &s){

  s.erase(0,s.find_first_not_of(L" \n\r\t"));

  s.erase(s.find_last_not_of(L" \n\r\t")+1);
}

CLuceneBackend::CLuceneBackend()
{
    //open indexes
    domain_index_locks locks = LuceneManager->getIndexLocks();
    domain_index_locks::iterator it;
    index_locks::iterator iit;

    for(it = locks.begin(); it !=  locks.end(); ++it) {
        for( iit = it->second.begin(); iit != it->second.end(); ++iit) {

            string idx_path(LuceneManager->getIndexRoot());
            idx_path += "/" + it->first + "/" + iit->first;

            if(!IndexReader::indexExists( idx_path.c_str() )) {
                throw std::runtime_error("CLucene: The index ("+idx_path+") does not exist");
            }


            try {
                indexes[idx_path]      = boost::shared_ptr<IndexSearcher>(new IndexSearcher(idx_path.c_str()));
                idx_versions[idx_path] = IndexReader::getCurrentVersion(idx_path.c_str());

            } catch(CLuceneError &e) {

                throw;
            }
        }
    }



    //Create fake client for redo logging and transaction purposes
    transport = boost::shared_ptr<TMemoryBuffer>(new TMemoryBuffer());

    boost::shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));

    faux_client = boost::shared_ptr<ThrudexClient>(new ThrudexClient(protocol));

}


CLuceneBackend::~CLuceneBackend()
{
    map<string, boost::shared_ptr<IndexSearcher> >::iterator it;

    for(it = indexes.begin(); it != indexes.end(); ++it) {
        it->second->close();
    }

    indexes.clear();
}

void CLuceneBackend::ping()
{

}

void CLuceneBackend::add(const DocMsg &d)
{
    vector<DocMsg> dv;

    dv.push_back(d);

    return this->addList( dv );
}

void CLuceneBackend::update(const DocMsg &d)
{
    vector<DocMsg> du;

    du.push_back(d);

    return this->updateList( du );
}

void CLuceneBackend::remove(const RemoveMsg &r)
{
    vector<RemoveMsg> dr;

    LOG4CXX::DEBUG("Removing: "+r.docid);

    dr.push_back(r);

    return this->removeList( dr );
}

QueryResponse CLuceneBackend::query(const QueryMsg &q)
{
    vector<QueryMsg> dq;

    dq.push_back(q);

    vector< QueryResponse > r;

    return this->queryList( dq )[0];
}

void CLuceneBackend::addList( const vector<DocMsg> &dv )
{
    LOG4CXX::DEBUG("Entering addList");

    if(dv.empty())
        return;

    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_addList(dv);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_addList(dv);

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_addList(dv);
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);

    RecoveryManager->addRedo( raw_msg, t->getId() );
}

void CLuceneBackend::_addList( const vector<DocMsg> &dv )
{

    if(dv.empty())
        return;

    vector< lucene::document::Document *> docs;

    string domain;
    string index;

    for(unsigned int i=0; i<dv.size(); i++) {

        if( i == 0 ) {
            domain = dv[i].domain;
            index  = dv[i].index;

            assert( LuceneManager->isValidIndex(domain,index) );

        } else if( domain != dv[i].domain || index != dv[i].index ){
            ThrudexException e;
            e.what   = "Currently, you can only add to one index per request";
            e.eclass = CRITICAL;

            throw e;
        }


        lucene::document::Document *d = new lucene::document::Document();

        LOG4CXX::DEBUG("Adding "+dv[i].docid);

        wstring docid     = build_wstring( dv[i].docid );
        assert( !docid.empty() );

        lucene::document::Field *f;


        f = lucene::document::Field::Keyword( L"id", docid.c_str() );

        d->add(*f);

        for( unsigned int j=0; j<dv[i].fields.size(); j++){
            LOG4CXX::DEBUG(dv[i].fields[j].name+":"+dv[i].fields[j].value);
            wstring key   = build_wstring( dv[i].fields[j].name   );
            wstring value = build_wstring( dv[i].fields[j].value );


            switch(dv[i].fields[j].stype){
                case KEYWORD:
                    LOG4CXX::DEBUG("Keyword");
                    f = lucene::document::Field::Keyword(key.c_str(),value.c_str());  break;
                case TEXT:
                    LOG4CXX::DEBUG("Text");
                    f = lucene::document::Field::Text(key.c_str(),value.c_str());     break;
                default:
                    LOG4CXX::DEBUG("UnStored");
                    f = lucene::document::Field::UnStored(key.c_str(),value.c_str()); break;
            };

            if(dv[i].fields[j].boost > 0){
                f->setBoost( dv[i].fields[j].boost );
            }


            d->add(*f);

            //If Sorted field then add _sort
            if(dv[i].fields[j].sortable){
                key += L"_sort";

                f = lucene::document::Field::Keyword(key.c_str(),value.c_str());

                d->add(*f);
            }
        }

        if(dv[i].boost > 0)
            d->setBoost(dv[i].boost);

        docs.push_back(d);
    }


    this->add(domain,index,docs);

    vector< lucene::document::Document *>::iterator i;
    for (i = docs.begin (); i < docs.end (); i++)
    {
        delete (*i);
    }

    return;
}

void CLuceneBackend::removeList( const vector<RemoveMsg> &rl )
{
    if(rl.empty())
        return;


    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_removeList(rl);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                  this->_removeList(rl);

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_removeList(rl);
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    RecoveryManager->addRedo( raw_msg, t->getId() );

    TransactionManager->endTransaction(t);
}

void CLuceneBackend::_removeList( const vector<RemoveMsg> &rl )
{
    LOG4CXX::DEBUG("Entering _removeList");

    if(rl.empty())
        return;

    vector<wstring> to_remove;

    string domain;
    string index;

    for(unsigned int i=0; i<rl.size(); i++){

        if( i == 0 ) {
            domain = rl[i].domain;
            index  = rl[i].index;

            assert( LuceneManager->isValidIndex(domain,index) );

        } else if( domain != rl[i].domain || index != rl[i].index ){
            ThrudexException e;
            e.what   = "Currently, you can only add to one index per request";
            e.eclass = CRITICAL;

            throw e;
        }


        LOG4CXX::DEBUG(string("Removing ")+rl[i].docid);
        wstring docid     = build_wstring( rl[i].docid );
        assert( !docid.empty() );

        to_remove.push_back(docid);
    }

    this->remove(domain,index,to_remove);

}

void CLuceneBackend::updateList( const vector<DocMsg> &ul)
{
    if(ul.empty())
        return;

    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_updateList(ul);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                  this->_updateList(ul);

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_updateList(ul);
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    RecoveryManager->addRedo( raw_msg, t->getId() );

    TransactionManager->endTransaction(t);
}

void CLuceneBackend::_updateList( const vector<DocMsg> &ul)
{
    if(ul.empty())
        return;


    vector<RemoveMsg> to_remove;
    for(unsigned int i=0; i<ul.size(); i++)
    {
        RemoveMsg r;

        r.index  = ul[i].index;
        r.domain = ul[i].domain;
        r.docid  = ul[i].docid;

        to_remove.push_back(r);
    }

    this->removeList(to_remove);
    this->addList(ul);
}


vector<QueryResponse> CLuceneBackend::queryList(const vector<QueryMsg> &q)
{
    vector<QueryResponse> ret;

    if(q.empty())
        return ret;


    string domain;
    string index;
    string idx_path;

    for(unsigned int i=0; i<q.size(); i++) {

        if( i == 0 ) {
            domain = q[i].domain;
            index  = q[i].index;

            LOG4CXX::DEBUG("Searching in: "+domain+"("+index+")");

            assert( LuceneManager->isValidIndex(domain,index) );

            idx_path = LuceneManager->getIndexRoot() +
                "/" + domain + "/" +index;

            if( LuceneManager->getIndexVersion(domain, index) > idx_versions[idx_path]) {
                indexes[idx_path]->close();

                indexes[idx_path] = boost::shared_ptr<IndexSearcher>(new IndexSearcher(idx_path.c_str()));

                idx_versions[idx_path] = IndexReader::getCurrentVersion(idx_path.c_str());
            }


        } else if( domain != q[i].domain || index != q[i].index ){
            ThrudexException e;
            e.what   = "Currently, you can only add to one index per request";
            e.eclass = CRITICAL;

            throw e;
        }


        Query *query;

        try{

            assert( !q[i].query.empty() );

            LOG4CXX::DEBUG(q[i].query);

            wstring wquery = build_wstring(q[i].query);

            query = QueryParser::parse( wquery.c_str(),L"id",&analyzer);

        } catch(CLuceneError &e) {

            ThrudexException ex;
            ex.what  = "Invalid query: '"+string(e.what())+"'";
            ex.eclass= CRITICAL;
            throw ex;

        } catch(runtime_error &e) {

            ThrudexException ex;
            ex.what  = "Invalid query: '"+string(e.what())+"'";
            ex.eclass= CRITICAL;
            throw ex;
        }


        Hits *h;
        Sort *lsort = NULL;


        if( q[i].sortby.empty() ){
            h = indexes[idx_path]->search(query);
        } else {


            LOG4CXX::DEBUG("Sorting by: "+q[i].sortby+" "+(q[i].desc ? "Descending": ""));

            wstring  sortby   = build_wstring( q[i].sortby+"_sort" );

            lsort = new Sort();
            lsort->setSort(  new SortField ( sortby.c_str(), SortField::STRING, q[i].desc ) );

            try {
                h = indexes[idx_path]->search(query,lsort);
            } catch(CLuceneError &e) {


                h = indexes[idx_path]->search(query);
            }
        }


        char buf[1024];

        QueryResponse r;
        r.total = h->length();

        int mlen = h->length();

        if( mlen > 0 ){

            if(q[i].randomize){

                map<int, bool> unique_lookup;

                for(int j=0; j<mlen && j<q[i].limit; j++){
                    int k = 0;

                    do{
                        k = rand() % mlen;
                    }while(unique_lookup.count(k));

                    lucene::document::Document *doc = &h->doc(k);

                    const wchar_t *id   = doc->get(_T("id"));

                    if(id == NULL){
                        assert(id != NULL);
                        continue;
                    } else {


                        STRCPY_TtoA(buf,id,1024);

                        LOG4CXX::DEBUG("ID: "+string(buf));
                        r.ids.push_back(buf);

                        unique_lookup[k] = true;
                    }
                }


            } else {

                for ( int j=q[i].offset;j<mlen && j<(q[i].offset+q[i].limit); j++ ) {

                    lucene::document::Document *doc = &h->doc(j);
                    const wchar_t *id   = doc->get(_T("id"));

                    if(id == NULL) {
                        assert(id != NULL);
                        continue;
                    } else {
                        STRCPY_TtoA(buf,id,1024);

                        LOG4CXX::DEBUG("ID: "+string(buf));
                        r.ids.push_back(buf);
                    }
                }
            }
        }

        ret.push_back(r);

        _CLDELETE(h);
        _CLDELETE(lsort);
        _CLDELETE(query);
    }

    return ret;
}


void CLuceneBackend::add( string &domain, string &index, vector<lucene::document::Document *> &to_add)
{
    //Writem if you got em
    domain_index_locks idx_locks = LuceneManager->getIndexLocks();
    boost::shared_ptr<Mutex> mutex = idx_locks[ domain ][ index ];

    //Lock and load (or add in this case)
    Guard guard( *mutex.get());

    boost::shared_ptr<IndexWriter> w = LuceneManager->getSharedIndexWriter(domain,index);

    for(unsigned int i=0; i<to_add.size(); i++) {
        w->addDocument(to_add[i]);
    }

    return;
}

void CLuceneBackend::remove(string &domain, string &index, vector<wstring> &to_remove )
{

    LOG4CXX::DEBUG("Entering remove");

    LOG4CXX::DEBUG("Calling remove on:"+domain+"("+index+")");

    //Writem if you got em
    domain_index_locks   idx_locks = LuceneManager->getIndexLocks();
    boost::shared_ptr<Mutex> mutex = idx_locks[ domain ][ index ];

    //Lock and load (or add in this case)
    Guard guard(*mutex.get());

    try {

        boost::shared_ptr<IndexReader> r = LuceneManager->getSharedIndexReader(domain,index);

        for(unsigned int i=0; i<to_remove.size(); i++) {


            Term *t = new Term(_T("id"), to_remove[i].c_str() );

            r->deleteTerm(t);
        }


    } catch(CLuceneError &ee) {

        ThrudexException e;
        e.what  = "Remove Error: '"+string(ee.what())+"'";
        e.eclass= CRITICAL;
        throw e;

    } catch(runtime_error &ee) {

        ThrudexException e;
        e.what  = "Remove Error: '"+string(ee.what())+"'";
        e.eclass= CRITICAL;
        throw e;
    }

    return;
}


void CLuceneBackend::optimize(const string &domain, const string &index)
{
    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_optimize(domain,index);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_optimize(domain,index);

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_optimize(domain,index);
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);


    //prob unnessisarry
    //RecoveryManager->addRedo( raw_msg, t->getId() );

}

void CLuceneBackend::_optimize(const string &domain, const string &index)
{
    domain_index_locks idx_locks = LuceneManager->getIndexLocks();
    boost::shared_ptr<Mutex> mutex = idx_locks[ domain ][ index ];

    Guard guard(*mutex.get());

    boost::shared_ptr<IndexWriter> w = LuceneManager->getSharedIndexWriter(domain,index);
    w->optimize();

    //By calling these we effectivly open and close the index
    LuceneManager->getSharedIndexReader(domain,index);

    return;
}


void CLuceneBackend::optimizeAll()
{
    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_optimizeAll();

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_optimizeAll();

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_optimizeAll();
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);

    //this is prob unnessasary
    //RecoveryManager->addRedo( raw_msg, t->getId() );
}

void CLuceneBackend::_optimizeAll()
{
    domain_index_locks idx_locks = LuceneManager->getIndexLocks();

    domain_index_locks::iterator  it;
    index_locks::iterator        iit;

    //Looks really bad but works.
    for(it=idx_locks.begin(); it!=idx_locks.end(); ++it){
        for(iit=it->second.begin(); iit!=it->second.end(); ++iit)
            this->_optimize(it->first,iit->first);
    }

    return;
}

void CLuceneBackend::commit(const string &domain, const string &index)
{
    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_commit(domain,index);

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_commit(domain,index);

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_commit(domain,index);
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);

    //this is prob unnessasary
    RecoveryManager->addRedo( raw_msg, t->getId() );

}

void CLuceneBackend::_commit(const string &domain, const string &index)
{
    domain_index_locks idx_locks = LuceneManager->getIndexLocks();
    boost::shared_ptr<Mutex> mutex = idx_locks[ domain ][ index ];

    LOG4CXX::DEBUG("Entering commit"+domain+"("+index+")");

    Guard guard(*mutex.get());

    //By calling these we effectivly open and close the index
    LuceneManager->getSharedIndexWriter(domain,index);
    LuceneManager->getSharedIndexReader(domain,index);

    return;
}

void CLuceneBackend::commitAll()
{
    boost::shared_ptr<Transaction> t = TransactionManager->beginTransaction();

    //Create raw message
    faux_client->send_commitAll();

    string raw_msg = transport->getBufferAsString();

    //clear for next input
    transport->resetBuffer();

    try{
        if( SpreadManager->isEnabled() ){

            memd->set( t->getId(), raw_msg, 120 );  //expires in 120 secs

            //Prepare the other clients
            if( SpreadManager->startTransaction(t) ){

                //actually store the result
                this->_commitAll();

            } else {
                ThrudexException e;
                e.what = "transaction error";
                throw e;
            }

        } else {

            //No spread means just write it
            this->_commitAll();
        }

    } catch (MemcacheException e) {
        LOG4CXX::ERROR("Memcached Error");
        throw;

    } catch (ThrudexException e){
        LOG4CXX::ERROR(string("Thrudex Exception ")+e.what);
        throw;

    } catch(...) {
        //TransactionManager->rollbackTransaction(t);

        TransactionManager->endTransaction(t);
        LOG4CXX::ERROR("Error saving transaction");
        return;
    }

    TransactionManager->endTransaction(t);


    RecoveryManager->addRedo( raw_msg, t->getId() );

}

void CLuceneBackend::_commitAll()
{
    domain_index_locks idx_locks = LuceneManager->getIndexLocks();

    domain_index_locks::iterator  it;
    index_locks::iterator        iit;

    //Looks bad but works.
    for(it=idx_locks.begin(); it!=idx_locks.end(); ++it){
        for(iit=it->second.begin(); iit!=it->second.end(); ++iit){
            this->_commit(it->first,iit->first);
        }
    }

    return;
}
