/**
 *
 **/

#ifndef _N_BACKEND_H_
#define _N_BACKEND_H_

#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "Thrudoc.h"
#include "ThrudocBackend.h"

class NBackend : public ThrudocBackend
{
    public:
        NBackend (std::vector<boost::shared_ptr<ThrudocBackend> > backends);
        ~NBackend ();

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

        std::vector<boost::shared_ptr<ThrudocBackend> > backends;
};

#endif
