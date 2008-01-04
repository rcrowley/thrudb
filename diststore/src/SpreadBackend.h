/**
 *
 **/

#ifndef _SPREAD_BACKEND_H_
#define _SPREAD_BACKEND_H_

#include <log4cxx/logger.h>
#include <set>
#include <sp.h>
#include <string>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include "DistStore.h"
#include "DistStoreBackend.h"

using namespace boost;
using namespace log4cxx;
using namespace diststore;
using namespace std;

class SpreadBackend : public DistStoreBackend
{
    public:
        SpreadBackend (const string & spread_name, 
                       const string & spread_private_name,
                       const string & spread_group,
                       shared_ptr<DistStoreBackend> backend);
        ~SpreadBackend ();

        vector<string> getTablenames ();
        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);
        string admin (const string & op, const string & data);
        void validate (const string * tablename, const string * key,
                       const string * value);

    private:
        static log4cxx::LoggerPtr logger;

        string spread_name;
        string spread_private_name;
        string spread_group;
        string spread_private_group;
        mailbox spread_mailbox;
        shared_ptr<DistStoreBackend> backend;
};

#endif
