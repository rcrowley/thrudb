#include "StatsBackend.h"

using namespace boost;
using namespace log4cxx;
using namespace std;
using namespace thrudex;

LoggerPtr StatsBackend::logger (Logger::getLogger ("StatsBackend"));

StatsBackend::StatsBackend(shared_ptr<ThrudexBackend> backend) :
  getIndices_count (0),
  put_count (0),
  remove_count (0),
  search_count (0),
  putList_count (0),
  removeList_count (0),
  searchList_count (0),
  admin_count (0)
{
  LOG4CXX_INFO (logger, "StatsBackend");

  this->set_backend (backend);
}

vector<string> StatsBackend::getIndices()
{
  ++getIndices_count;
  return this->get_backend ()->getIndices ();
}

void StatsBackend::put(const Document &d)
{
  ++put_count;
  return this->get_backend ()->put (d);
}

void StatsBackend::remove(const Element &e)
{
  ++remove_count;
  return this->get_backend ()->remove (e);
}

void StatsBackend::search(const SearchQuery &s, SearchResponse &r)
{
  ++search_count;
  return this->get_backend ()->search (s, r);
}

vector<ThrudexException> StatsBackend::putList(const vector<Document> &documents)
{
  ++putList_count;
  return this->get_backend ()->putList (documents);
}

vector<ThrudexException> StatsBackend::removeList(const vector<Element> &elements)
{
  ++removeList_count;
  return this->get_backend ()->removeList (elements);
}

vector<SearchResponse> StatsBackend::searchList(const vector<SearchQuery> &q)
{
  ++searchList_count;
  return this->get_backend ()->searchList (q);
}

string StatsBackend::admin(const string &op, const string &data)
{
  if (op == "stats")
  {
    char buf[1024];
    // casts are require to prevent POD type problems with atomic_count
    sprintf (buf, "getIndices_count=%lu,put_count=%lu,remove_count=%lu,search_count=%lu,putList_count=%lu,removeList_count=%lu,searchList=%lu,admin=%lu",
             (long)getIndices_count, (long)put_count, (long)remove_count, 
             (long)search_count, (long)putList_count, (long)removeList_count,
             (long)searchList_count, (long)admin_count);
    return string (buf);
  }
  ++admin_count;
  return this->get_backend ()->admin (op, data);
}
