#ifndef __THRUDEX_BACKEND_H__
#define __THRUDEX_BACKEND_H__

#include "Thrudex.h"

#include <log4cxx/logger.h>

class ThrudexBackend
{
 public:
    ThrudexBackend();
    virtual ~ThrudexBackend();

    virtual std::vector<std::string> getIndices() = 0;

    virtual void put(const thrudex::Document &d) = 0;

    virtual void remove(const thrudex::Element &e) = 0;

    virtual void search(const thrudex::SearchQuery &s, thrudex::SearchResponse &r) = 0;

    virtual std::vector<thrudex::ThrudexException> putList(const std::vector<thrudex::Document> &documents);

    virtual std::vector<thrudex::ThrudexException> removeList(const std::vector<thrudex::Element> &elements);

    virtual std::vector<thrudex::SearchResponse> searchList(const std::vector<thrudex::SearchQuery> &q);

    virtual std::string admin(const std::string &op, const std::string &data);

 private:
    static log4cxx::LoggerPtr logger;
};

#endif


