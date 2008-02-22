#ifndef __STATS_BACKEND_H__
#define __STATS_BACKEND_H__

#include "ThrudexPassthruBackend.h"

#include <log4cxx/logger.h>
#include <boost/detail/atomic_count.hpp>

class StatsBackend : public ThrudexPassthruBackend
{
  public:
    StatsBackend(boost::shared_ptr<ThrudexBackend> backend);

    std::vector<std::string> getIndices();

    void put(const thrudex::Document &d);

    void remove(const thrudex::Element &e);

    void search(const thrudex::SearchQuery &s, thrudex::SearchResponse &r);

    std::vector<thrudex::ThrudexException> putList(const std::vector<thrudex::Document> &documents);

    std::vector<thrudex::ThrudexException> removeList(const std::vector<thrudex::Element> &elements);

    std::vector<thrudex::SearchResponse> searchList(const std::vector<thrudex::SearchQuery> &q);

    std::string admin(const std::string &op, const std::string &data);

  private:
    static log4cxx::LoggerPtr logger;

    boost::detail::atomic_count getIndices_count;
    boost::detail::atomic_count put_count;
    boost::detail::atomic_count remove_count;
    boost::detail::atomic_count search_count;
    boost::detail::atomic_count putList_count;
    boost::detail::atomic_count removeList_count;
    boost::detail::atomic_count searchList_count;
    boost::detail::atomic_count admin_count;
};

#endif /* __STATS_BACKEND_H__ */
