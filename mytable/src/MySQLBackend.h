/**
 *
 **/

#ifndef _MYSQL_BACKEND_H_
#define _MYSQL_BACKEND_H_

#include <string>
#include <log4cxx/logger.h>

#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "MyTable.h"
#include "MyTableBackend.h"
#include "mysql_glue.h"

using namespace log4cxx;
using namespace mysql;
using namespace mytable;
using namespace std;

struct FindReturn
{
    Connection * connection;
    string data_tablename;
};

class MySQLBackend : public MyTableBackend
{
    public:
        MySQLBackend();
        ~MySQLBackend();

        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed, 
                           int32_t count);

    protected:

        FindReturn find_and_checkout (const string & tablename, 
                                      const string & key );
        FindReturn find_next_and_checkout (const string & tablename,
                                           const string & current_datatablename);
        void checkin (Connection * connection);

        static string master_hostname;
        static int master_port;
        static string master_db;
        static string master_username;
        static string master_password;

    private:
        static log4cxx::LoggerPtr logger;

        FindReturn and_checkout (Connection * connection, 
                                 PreparedStatement * statement);
        string scan_helper (ScanResponse & scan_response, 
                            FindReturn & find_return, const string & offset, 
                            int32_t count);
};

#endif
