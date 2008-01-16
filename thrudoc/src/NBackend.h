/**
 *
 **/

#ifndef _N_BACKEND_H_
#define _N_BACKEND_H_

#include <log4cxx/logger.h>
#include <set>
#include <string>
#include "DistStore.h"
#include "DistStoreBackend.h"

class NBackend : public DistStoreBackend
{
    public:
        NBackend (std::vector<boost::shared_ptr<DistStoreBackend> > backends);
        ~NBackend ();

        std::vector<std::string> getTablenames ();
        std::string get (const std::string & tablename,
                         const std::string & key);
        void put (const std::string & tablename, const std::string & key,
                  const std::string & value);
        void remove (const std::string & tablename, const std::string & key);
        diststore::ScanResponse scan (const std::string & tablename,
                                      const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & tablename, const std::string * key,
                       const std::string * value);

    private:
        static log4cxx::LoggerPtr logger;

        std::vector<boost::shared_ptr<DistStoreBackend> > backends;
};

#endif
