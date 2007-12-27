/**
 *
 **/

#ifndef _MYSQL_BACKEND_H_
#define _MYSQL_BACKEND_H_

#include <string>
#include <set>
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

class Partition
{
    public:
        static bool greater (Partition * a, Partition * b)
        {
            return strcmp (a->get_end (), b->get_end ()) <= 0;
        }

        Partition (const string & end)
        {
            memcpy (this->end, end.c_str (), end.length ());
        }

        Partition (PartitionsResults * partition_results)
        {
            strncpy (this->end, partition_results->get_end (),
                     sizeof (this->end));
            strncpy (this->host, partition_results->get_host (),
                     sizeof (this->host));
            strncpy (this->db, partition_results->get_db (),
                     sizeof (this->db));
            strncpy (this->table, partition_results->get_table (),
                     sizeof (this->table));
        }

        const char * get_end ()
        {
            return this->end;
        }

        const char * get_host ()
        {
            return this->host;
        }

        const char * get_db ()
        {
            return this->db;
        }

        const char * get_table ()
        {
            return this->table;
        }

    protected:
        char end[MYSQL_BACKEND_MAX_KEY_SIZE];
        char host[MYSQL_BACKEND_MAX_HOST_SIZE];
        char db[MYSQL_BACKEND_MAX_DB_SIZE];
        char table[MYSQL_BACKEND_MAX_TABLE_SIZE];
};

class MySQLBackend : public MyTableBackend
{
    public:
        MySQLBackend ();

        string get (const string & tablename, const string & key );
        void put (const string & tablename, const string & key, 
                  const string & value);
        void remove (const string & tablename, const string & key );
        ScanResponse scan (const string & tablename, const string & seed,
                           int32_t count);

        string admin (const string & op, const string & data);

    protected:

        static map<string, set<Partition*, bool(*)(Partition*, Partition*)>* > 
            partitions;
        static string master_hostname;
        static int master_port;
        static string master_db;
        static string master_username;
        static string master_password;

        FindReturn find_and_checkout (const string & tablename,
                                      const string & key );
        FindReturn find_next_and_checkout (const string & tablename,
                                           const string & current_datatablename);
        void checkin (Connection * connection);

    private:
        static log4cxx::LoggerPtr logger;

        void load_partitions (const string & tablename);

        FindReturn and_checkout (Connection * connection,
                                 PreparedStatement * statement);
        string scan_helper (ScanResponse & scan_response,
                            FindReturn & find_return, const string & offset,
                            int32_t count);
};

#endif
