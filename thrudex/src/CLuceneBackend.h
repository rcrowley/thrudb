#ifndef __CLUCENE_BACKEND_H__
#define __CLUCENE_BACKEND_H__

#include "ThrudexBackend.h"

#include <log4cxx/logger.h>

#include <boost/shared_ptr.hpp>

#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Util.h>

#include <iostream>
#include <stdexcept>
#include <string>
#include <map>
#include <vector>

#include "CLuceneIndex.h"

class CLuceneBackend : public ThrudexBackend
{
 public:
    CLuceneBackend(const std::string &idx_root);
    ~CLuceneBackend();

    std::vector<std::string>  getIndices();

    void  put   (const thrudex::Document    &d);
    void  remove(const thrudex::Element     &e);
    void  search(const thrudex::SearchQuery &s, thrudex::SearchResponse &r);

    std::string admin(const std::string &op, const std::string &data);

 private:

    static log4cxx::LoggerPtr logger;

    void  addIndex     (const std::string &index);
    bool  isValidIndex (const std::string &index);

    const std::string   idx_root;       ///< from conf file

    std::map<std::string, boost::shared_ptr<CLuceneIndex> > index_cache;

    boost::shared_ptr<lucene::analysis::Analyzer> analyzer;
    facebook::thrift::concurrency::ReadWriteMutex mutex;
};

#endif
