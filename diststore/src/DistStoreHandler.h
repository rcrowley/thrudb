/**
 *
 **/

#ifndef __DISTSTORE_HANDLER__
#define __DISTSTORE_HANDLER__

#include "DistStore.h"
#include "DistStoreBackend.h"

#include <string>
#include <log4cxx/logger.h>
#include <libmemcached/memcached.h>

class DistStoreHandler : virtual public diststore::DistStoreIf {
    public:
        DistStoreHandler (boost::shared_ptr<DistStoreBackend> backend);

        void getTablenames (std::vector<std::string> & _return);
        void put (const std::string & tablename, const std::string & key,
                  const std::string & value);
        void putValue (std::string & _return, const std::string & tablename,
                       const std::string & value);
        void get (std::string & _return, const std::string & tablename,
                  const std::string & key);
        void remove (const std::string & tablename, const std::string & key);
        void scan (diststore::ScanResponse & _return,
                   const std::string & tablename,
                   const std::string & seed, int32_t count);
        void admin (std::string & _return, const std::string & op,
                    const std::string & data);

    private:
        static log4cxx::LoggerPtr logger;

        boost::shared_ptr<DistStoreBackend> backend;
};

#endif
