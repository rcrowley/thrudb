/**
 *
 **/

#ifndef _MYTABLE_BACKEND_H_
#define _MYTABLE_BACKEND_H_

#include <string>
#include "MyTable.h"

using namespace std;
using namespace mytable;

/**
 *
 **/

class MyTableBackend
{
    public:
        virtual ~MyTableBackend () {};

        virtual string get (const string & tablename, const string & key ) = 0;
        virtual void put (const string & tablename, const string & key,
                          const string & value) = 0;
        virtual void remove (const string & tablename, const string & key ) = 0;
        virtual ScanResponse scan (const string & tablename,
                                   const string & seed, int32_t count) = 0;
        virtual string admin (const string & op, const string & data) = 0;
};


#endif
