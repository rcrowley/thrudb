/**
 *
 **/

#ifndef _DISTSTORE_BACKEND_H_
#define _DISTSTORE_BACKEND_H_

#include <string>
#include "DistStore.h"

using namespace std;
using namespace diststore;

/**
 *
 **/

class DistStoreBackend
{
    public:
        virtual ~DistStoreBackend () {};

        virtual vector<string> getTablenames () = 0;
        virtual string get (const string & tablename, const string & key ) = 0;
        virtual void put (const string & tablename, const string & key,
                          const string & value) = 0;
        virtual void remove (const string & tablename, const string & key ) = 0;
        virtual ScanResponse scan (const string & tablename,
                                   const string & seed, int32_t count) = 0;
        virtual string admin (const string & op, const string & data) = 0;

        // will be called to validate input params through the backend.
        // should be able to handle NULL's approrpriately
        virtual void validate (const string * tablename, const string * key,
                               const string * value) = 0;
};


#endif
