
/**
 *
 **/

#ifndef _STATS_BACKEND_H_
#define _STATS_BACKEND_H_

#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "Thrudoc.h"
#include "ThrudocBackend.h"

class StatsBackend : public ThrudocBackend
{
    public:
        StatsBackend (boost::shared_ptr<ThrudocBackend> backend);
        ~StatsBackend ();

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    private:
        static log4cxx::LoggerPtr logger;
        unsigned long get_buckets_count;
        unsigned long get_count;
        unsigned long put_count;
        unsigned long remove_count;
        unsigned long scan_count;
        unsigned long admin_count;

        boost::shared_ptr<ThrudocBackend> backend;
};

#endif
