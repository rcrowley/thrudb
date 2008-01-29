/**
 *
 **/

#ifndef _N_BACKEND_H_
#define _N_BACKEND_H_

#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

class NBackend : public ThrudocPassthruBackend
{
    public:
        NBackend (std::vector<boost::shared_ptr<ThrudocBackend> > backends);

        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    private:
        static log4cxx::LoggerPtr logger;

        std::vector<boost::shared_ptr<ThrudocBackend> > backends;
};

#endif
