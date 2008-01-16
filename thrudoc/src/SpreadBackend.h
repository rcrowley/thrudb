/**
 *
 **/

#ifndef _SPREAD_BACKEND_H_
#define _SPREAD_BACKEND_H_

#if HAVE_LIBSPREAD

#include <log4cxx/logger.h>
#include <set>
#include <sp.h>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include "Thrudoc.h"
#include "ThrudocBackend.h"

class SpreadBackend : public ThrudocBackend
{
    public:
        SpreadBackend (const std::string & spread_name, 
                       const std::string & spread_private_name,
                       const std::string & spread_group,
                       boost::shared_ptr<ThrudocBackend> backend);
        ~SpreadBackend ();

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

        std::string spread_name;
        std::string spread_private_name;
        std::string spread_group;
        std::string spread_private_group;
        mailbox spread_mailbox;
        boost::shared_ptr<ThrudocBackend> backend;
};

#endif /* HAVE_LIBSPREAD */

#endif /* _SPREAD_BACKEND_H_ */
