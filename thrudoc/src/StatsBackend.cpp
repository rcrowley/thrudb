
#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "StatsBackend.h"

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

/* TODO
 * - this isn't thread-safe, make it so... until then counts will be (very) 
 *   slightly off/under. doing thread local counts summed up during admin calls
 *   is my personal pick for how to do this, but i'd really like to know a nice
 *   lightweight way to get at the counts in the other threads...
 */

// private
LoggerPtr StatsBackend::logger (Logger::getLogger ("StatsBackend"));

StatsBackend::StatsBackend (shared_ptr<ThrudocBackend> backend)
{
    LOG4CXX_INFO (logger, string ("StatsBackend"));

    this->set_backend (backend);
    get_buckets_count = 0;
    get_count = 0;
    put_count = 0;
    remove_count = 0;
    scan_count = 0;
    admin_count = 0;
    putList_count = 0;
    getList_count = 0;
    removeList_count = 0;
}

vector<string> StatsBackend::getBuckets ()
{
    ++get_buckets_count;
    return this->get_backend ()->getBuckets ();
}

string StatsBackend::get (const string & bucket, const string & key )
{
    ++get_count;
    return this->get_backend ()->get (bucket, key);
}

void StatsBackend::put (const string & bucket, const string & key, 
                        const string & value)
{
    ++put_count;
    this->get_backend ()->put (bucket, key, value);
}

void StatsBackend::remove (const string & bucket, const string & key )
{
    ++remove_count;
    this->get_backend ()->remove (bucket, key);
}

ScanResponse StatsBackend::scan (const string & bucket,
                                 const string & seed, int32_t count)
{
    ++scan_count;
    return this->get_backend ()->scan (bucket, seed, count);
}

string StatsBackend::admin (const string & op, const string & data)
{
    ++admin_count;
    if (op == "stats")
    {
        char buf[1024];
        sprintf (buf, "get_count=%lu,put_count=%lu,remove_count=%lu,scan_count=%lu,admin_count=%lu,putList_count=%lu,getList_count=%lu,removeList_count=%lu",
                 get_count, put_count, remove_count, scan_count, admin_count,
                 putList_count, getList_count, removeList_count);
        return string (buf);
    }
    return this->get_backend ()->admin (op, data);
}

vector<ThrudocException> StatsBackend::putList
(const vector<Element> & elements)
{
    ++putList_count;
    return this->get_backend ()->putList (elements);
}

vector<ListResponse> StatsBackend::getList
(const vector<Element> & elements)
{
    ++getList_count;
    return this->get_backend ()->getList (elements);
}

vector<ThrudocException> StatsBackend::removeList
(const vector<Element> & elements)
{
    ++removeList_count;
    return this->get_backend ()->removeList (elements);
}

void StatsBackend::validate (const string & bucket, const string * key, 
                             const string * value)
{
    this->get_backend ()->validate (bucket, key, value);
}
