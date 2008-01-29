/**
 *
 **/

#ifndef _SPREAD_BACKEND_H_
#define _SPREAD_BACKEND_H_

#if HAVE_LIBSPREAD

#include "Thrudoc.h"
#include "ThrudocPassthruBackend.h"

#include <log4cxx/logger.h>
#include <protocol/TBinaryProtocol.h>
#include <set>
#include <sp.h>
#include <string>
#include <transport/TTransportUtils.h>

class SpreadBackend : public ThrudocPassthruBackend
{
    public:
        SpreadBackend (boost::shared_ptr<ThrudocBackend> backend,
                       const std::string & spread_name, 
                       const std::string & spread_private_name,
                       const std::string & spread_group);
        ~SpreadBackend ();

        void put (const std::string & bucket, const std::string & key, 
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);

    private:
        static log4cxx::LoggerPtr logger;

        std::string spread_name;
        std::string spread_private_name;
        std::string spread_group;
        std::string spread_private_group;
        mailbox spread_mailbox;
};

#endif /* HAVE_LIBSPREAD */

#endif /* _SPREAD_BACKEND_H_ */
