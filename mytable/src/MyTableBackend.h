/**
 *
 **/

#ifndef _MYTABLE_BACKEND_H_
#define _MYTABLE_BACKEND_H_

#include <string>

using namespace std;

/**
 *
 **/

class MyTableBackend
{
    public:
        virtual ~MyTableBackend () {};

        virtual string get (const string & tablename, const string & key ) = 0;
        virtual void put (const string & tablename, const string & key, const string & value) = 0;
        virtual void remove (const string & tablename, const string & key ) = 0;
        virtual vector<string> scan (const string & tablename, const string & seed, int32_t count) = 0;
};


#endif
