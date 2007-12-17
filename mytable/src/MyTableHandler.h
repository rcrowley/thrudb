/**
 *
 **/

#ifndef __MYTABLE_HANDLER__
#define __MYTABLE_HANDLER__

#include "MyTable.h"
#include "MyTableBackend.h"

#include <string>
#include <log4cxx/logger.h>

using namespace std;
using namespace facebook::thrift;
using namespace mytable;

class MyTableHandler : virtual public MyTableIf {
    public:
        MyTableHandler (boost::shared_ptr<MyTableBackend> backend);

        void put(const string & tablename, const string & key, const string & value);
        void get(string & _return, const string & tablename, const string & key);
        void remove(const string & tablename, const string & key);
        void scan (ScanResponse & _return, const string & tablename, const string & seed, int32_t count);

    private:
        MyTableHandler(){};

        boost::shared_ptr<MyTableBackend> backend;

        static log4cxx::LoggerPtr logger;
};


#endif
