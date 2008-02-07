#ifndef __THRUDEX_HANDLER_H__
#define __THRUDEX_HANDLER_H__

#include "Thrudex.h"
#include "ThrudexBackend.h"

#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>

class ThrudexHandler : virtual public thrudex::ThrudexIf
{
 public:
    ThrudexHandler(boost::shared_ptr<ThrudexBackend> backend);

    void ping();

    void getIndices(std::vector<std::string> &_return);

    void put(const thrudex::Document &d);

    void remove(const thrudex::Element &e);

    void search(thrudex::SearchResponse &_return, const thrudex::SearchQuery &s);

    void putList(std::vector<thrudex::ThrudexException> &_return, const std::vector<thrudex::Document> &documents);

    void removeList(std::vector<thrudex::ThrudexException> &_return, const std::vector<thrudex::Element> &elements);

    void searchList(std::vector<thrudex::SearchResponse> &_return, const std::vector<thrudex::SearchQuery> &q);

    void admin(std::string &_return, const std::string &op, const std::string &data);

 private:
    static log4cxx::LoggerPtr logger;

    boost::shared_ptr<ThrudexBackend> backend;

};

#endif


