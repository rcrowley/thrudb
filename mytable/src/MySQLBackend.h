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
    string datatable;
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
            strncpy (this->end, end.c_str (), sizeof (this->end));
        }

        Partition (PartitionsResults * partition_results)
        {
            strncpy (this->end, partition_results->get_end (),
                     sizeof (this->end));
            strncpy (this->host, partition_results->get_host (),
                     sizeof (this->host));
            strncpy (this->db, partition_results->get_db (),
                     sizeof (this->db));
            strncpy (this->datatable, partition_results->get_datatable (),
                     sizeof (this->datatable));
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

        const char * get_datatable ()
        {
            return this->datatable;
        }

    protected:
        char end[MYSQL_BACKEND_MAX_KEY_SIZE];
        char host[MYSQL_BACKEND_MAX_HOST_SIZE];
        char db[MYSQL_BACKEND_MAX_DB_SIZE];
        char datatable[MYSQL_BACKEND_MAX_DATATABLE_SIZE];
};

class MySQLBackend : public MyTableBackend
{
    public:
        MySQLBackend ();

        vector<string> getTablenames ();
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
        Connection * get_connection(const char * hostname, const char * db);

    private:
        static log4cxx::LoggerPtr logger;
        static pthread_key_t connections_key;

        set<Partition*, bool(*)(Partition*, Partition*)> * 
            load_partitions (const string & tablename);

        FindReturn and_checkout (Connection * connection,
                                 PreparedStatement * statement);
        string scan_helper (ScanResponse & scan_response,
                            FindReturn & find_return, const string & offset,
                            int32_t count);
};

#endif
