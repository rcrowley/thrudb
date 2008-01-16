/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#ifndef __LUCENE_MANAGER__
#define __LUCENE_MANAGER__

#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Util.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <map>
#include <vector>
#include <CLucene.h>
#include <boost/shared_ptr.hpp>


using namespace std;
using namespace facebook::thrift::concurrency;

using namespace lucene::index;
using namespace lucene::store;
using namespace lucene::search;
using namespace lucene::analysis;
using namespace lucene::util;
using namespace lucene::queryParser;
using namespace lucene::document;

//Types
typedef map<string, boost::shared_ptr<Mutex> >        index_locks;
typedef map<string, index_locks >                     domain_index_locks;

typedef map<string, boost::shared_ptr<IndexReader> >  index_readers;
typedef map<string, index_readers >                   domain_index_readers;

typedef map<string, boost::shared_ptr<IndexWriter> >  index_writers;
typedef map<string, index_writers >                   domain_index_writers;

typedef map<string, boost::shared_ptr<Analyzer> >     index_analyzers;
typedef map<string, index_analyzers >                 domain_index_analyzers;


//Singleton for lucene

class _LuceneManager
{
 public:
    void   startup();
    void   startup(const string &index_root, map<string,vector<string> > &indexes);
    void   destroy();

    void   addIndex( const string &domain, const string &index);

    void   setIndexRoot(const string &index_root );
    string getIndexRoot( );

    domain_index_locks getIndexLocks();
    bool         isValidIndex(const string &domain, const string &index);
    boost::shared_ptr<IndexReader> getSharedIndexReader(const string &domain, const string &index);
    boost::shared_ptr<IndexWriter> getSharedIndexWriter(const string &domain, const string &index);
    int64_t      getIndexVersion(const string &domain, const string &index);

    static _LuceneManager* instance();

private:
    _LuceneManager(){};

    domain_index_locks     idx_locks;      ///<keeps mutex locks for shared index access
    domain_index_readers   idx_readers;    ///<shared readers (for deletes)
    domain_index_writers   idx_writers;    ///<shared writers (for additions)
    domain_index_analyzers idx_analyzers;  ///<shared analyzers (for faster indexing)

    Mutex mutex;          ///< for singleton synch
    string index_root;    ///< from conf file

    static _LuceneManager* pInstance;
    static Mutex _mutex;
};


//Singleton shortcut
#define LuceneManager _LuceneManager::instance()

#endif
