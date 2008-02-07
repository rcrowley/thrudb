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



//Singleton for CLucene writes

class _LuceneManager
{
 public:
    void   startup();
    void   destroy();

    void   addIndex(const string &index);

    //Maintain a bloom? or allow duplicates and periodically clean up based on ts...
    bloom_filter *getBloom(const string &index);




    static _LuceneManager* instance();

private:
    _LuceneManager(){};

    index_locks     idx_locks;      ///<keeps mutex locks for shared index access
    index_readers   idx_readers;    ///<shared readers (for deletes)
    index_writers   idx_writers;    ///<shared writers (for additions)
    index_analyzers idx_analyzers;  ///<shared analyzers (for faster indexing)

    Mutex  mutex;          ///< for singleton synch
    string index_root;     ///< from conf file

    static _LuceneManager* pInstance;
    static Mutex _mutex;
};


//Singleton shortcut
#define LuceneManager _LuceneManager::instance()

#endif
