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

#include "LuceneManager.h"

#include <utils.h>

_LuceneManager* _LuceneManager::pInstance = 0;
Mutex           _LuceneManager::_mutex    = Mutex();

_LuceneManager* _LuceneManager::instance()
{
    if(pInstance == 0){
        Guard guard(_mutex);

        if(pInstance == 0){
            pInstance = new _LuceneManager();
        }
    }

    return pInstance;
}


void _LuceneManager::setIndexRoot(const string &index_root )
{
    this->index_root = index_root;
}

string _LuceneManager::getIndexRoot( )
{
    return index_root;
}


void _LuceneManager::startup()
{
    //Nothing todo
}

void _LuceneManager::startup(const string &index_root, map<string,vector<string> > &indexes)
{

    this->index_root = index_root;

    map<string,vector<string> >::iterator it;

    for(it = indexes.begin(); it != indexes.end(); ++it){
        for(unsigned int i=0; i<it->second.size(); i++)
            this->addIndex(it->first,it->second[i]);
    }
}

void _LuceneManager::destroy()
{

    //Close Readers
    domain_index_readers::iterator itr;
    index_readers::iterator iitr;

    for(itr = idx_readers.begin(); itr != idx_readers.end(); ++itr) {
        for( iitr = itr->second.begin(); iitr != itr->second.end(); ++iitr) {

            if( iitr->second != NULL ) {
                iitr->second->close();
            }
        }
    }
    idx_readers.clear();


    //Close Writers
    domain_index_writers::iterator itw;
    index_writers::iterator iitw;

    for(itw = idx_writers.begin(); itw != idx_writers.end(); ++itw) {
        for( iitw = itw->second.begin(); iitw != itw->second.end(); ++iitw) {

            if( iitw->second != NULL ) {
                iitw->second->close();
            }
        }
    }
    idx_writers.clear();


    //Delete Analyzers
    idx_analyzers.clear();


    //Delete Mutex locks
    idx_locks.clear();

}

void _LuceneManager::addIndex( const string &domain, const string &index )
{

    //is index already open?
    if( this->isValidIndex( domain, index ) )
        return;

    string idx_path(index_root);

    //Make sure index dir exists
    if(!directory_exists(index_root))
        mkdir(index_root.c_str(), 0777L );


    idx_path += "/" + domain;

    //Make sure domain dir exists
    if ( !IndexReader::indexExists(idx_path.c_str()) )
        mkdir(idx_path.c_str(), 0777L );


    idx_path += "/" + index;

    try {
        if ( IndexReader::indexExists(idx_path.c_str()) ) {

            if ( IndexReader::isLocked(idx_path.c_str()) )
                IndexReader::unlock(idx_path.c_str());

        } else {
            standard::StandardAnalyzer a;
            IndexWriter w(idx_path.c_str(),&a,true,true);
            w.close();

            cerr<<"Added "<<domain<<" "<<index<<endl;
        }

        //Add it to the config
        idx_locks   [domain][index] = boost::shared_ptr<Mutex>(new Mutex());
        idx_readers [domain][index] = boost::shared_ptr<IndexReader>();
        idx_writers [domain][index] = boost::shared_ptr<IndexWriter>();


    } catch(CLuceneError &e) {
        cerr<<"Clucene Exception while creating index"<<idx_path<<":"<<e.what()<<endl;
        throw;
    } catch(...) {
        cerr<<"Index base path is invalid, '"<<index_root<<"' check it.";
        throw;
    }

}


domain_index_locks _LuceneManager::getIndexLocks()
{
    return idx_locks;
}


bool _LuceneManager::isValidIndex(const string &domain, const string &index)
{
    if( idx_locks.count(domain) == 0 || idx_locks[domain].count(index) == 0 )
        return false;

    return true;
}


boost::shared_ptr<IndexReader> _LuceneManager::getSharedIndexReader(const string &domain, const string &index )
{
    Guard guard(mutex);

    if(!this->isValidIndex(domain,index))
        throw std::runtime_error("Invalid index name: '"+index+"' for domain: '"+domain+"'");

    //return it if it exists. upto calling code to play nice by using assoc mutex
    if( idx_readers[domain][index] != boost::shared_ptr<IndexReader>() ) {
        return idx_readers[domain][index];
    }

    string idx_path(index_root);
    idx_path += "/" + domain + "/" + index;

    if(!IndexReader::indexExists( idx_path.c_str() )) {
        throw std::runtime_error("Index dne. domain '"+idx_path+"'");
    }

    //close writer if open
    if( idx_writers[domain][index] != boost::shared_ptr<IndexWriter>() ) {

        idx_writers[domain][index]->close();

        idx_writers[domain][index] = boost::shared_ptr<IndexWriter>();
    }

    idx_readers[domain][index] = boost::shared_ptr<IndexReader>(IndexReader::open(idx_path.c_str()));

    return idx_readers[domain][index];
}

boost::shared_ptr<IndexWriter> _LuceneManager::getSharedIndexWriter( const string &domain, const string &index )
{
    Guard guard(mutex);

    bool create_idx = false;

    if(!this->isValidIndex(domain,index))
        throw std::runtime_error("Invalid index name: '"+index+"' for domain: '"+domain+"'");

    //return it if it exists. upto calling code to play nice by using assoc mutex
    if( idx_writers[domain][index] != boost::shared_ptr<IndexWriter>() ) {
        return idx_writers[domain][index];
    }

    //close reader if open
    if( idx_readers[domain][index] != boost::shared_ptr<IndexReader>() ) {

        idx_readers[domain][index]->close();

        idx_readers[domain][index] = boost::shared_ptr<IndexReader>();
    }

    string idx_path(index_root);
    idx_path += "/" + domain + "/" + index;

    if(idx_analyzers.count(domain) == 0 || idx_analyzers[domain].count(index) == 0)
        idx_analyzers[domain][index] = boost::shared_ptr<Analyzer>(new standard::StandardAnalyzer());

    boost::shared_ptr<IndexWriter> w(new IndexWriter(idx_path.c_str(),idx_analyzers[domain][index].get(),create_idx));

    //Put these in conf file
    w->setMergeFactor(1000);
    w->setMaxMergeDocs(9999999);
    w->setMinMergeDocs(1000);

    idx_writers[domain][index]   = w;

    return idx_writers[domain][index];
}

int64_t _LuceneManager::getIndexVersion(const string &domain, const string &index)
{
    Guard guard(mutex);

    if(this->isValidIndex(domain, index)) {

        string idx_path(index_root);
        idx_path += "/" + domain + "/" + index;

        return IndexReader::getCurrentVersion(idx_path.c_str());
    }

    throw std::runtime_error("Invalid index");
}



