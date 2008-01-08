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

using namespace boost;
using namespace log4cxx;
using namespace diststore;
using namespace std;

class NBackend : public DistStoreBackend
{
    public:
        NBackend (vector<shared_ptr<DistStoreBackend> > backends);
        ~NBackend ();

        vector<string> getTablenames ();
        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);
        string admin (const string & op, const string & data);
        void validate (const string & tablename, const string * key,
                       const string * value);

    private:
        static log4cxx::LoggerPtr logger;

        vector<shared_ptr<DistStoreBackend> > backends;
};

#endif
