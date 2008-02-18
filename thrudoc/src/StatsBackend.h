
/**
 *
 **/

#ifndef _STATS_BACKEND_H_
#define _STATS_BACKEND_H_

#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

#include <boost/detail/atomic_count.hpp>
#include <log4cxx/logger.h>
#include <set>
#include <string>

// TODO: - make a non atomic_count version of this that will be useable on
// systems without good atomic_count support

/* NOTE: this uses boost's atomic_count and on some systems that makes use of
 * mutexes's. you should be safe when compiled with gcc or on win32. otherwise
 * you probably want to disable this backend. */
class StatsBackend : public ThrudocPassthruBackend
{
    public:
        StatsBackend (boost::shared_ptr<ThrudocBackend> backend);

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        std::vector<thrudoc::ThrudocException> putList
            (const std::vector<thrudoc::Element> & elements);
        std::vector<thrudoc::ListResponse> getList
            (const std::vector<thrudoc::Element> & elements);
        std::vector<thrudoc::ThrudocException> removeList
            (const std::vector<thrudoc::Element> & elements);
        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    private:
        static log4cxx::LoggerPtr logger;

        boost::detail::atomic_count get_buckets_count;
        boost::detail::atomic_count get_count;
        boost::detail::atomic_count put_count;
        boost::detail::atomic_count remove_count;
        boost::detail::atomic_count scan_count;
        boost::detail::atomic_count admin_count;
        boost::detail::atomic_count putList_count;
        boost::detail::atomic_count getList_count;
        boost::detail::atomic_count removeList_count;
};

#endif
