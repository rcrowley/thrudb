/**
 *
 **/

#ifndef _MYSQL_BACKEND_H_
#define _MYSQL_BACKEND_H_

#if HAVE_LIBMYSQLCLIENT_R

#include "Thrudoc.h"
#include "ThrudocBackend.h"
#include "mysql_glue.h"

#include <Hashing.h>
#include <log4cxx/logger.h>
#include <set>
#include <string>

struct FindReturn
{
    mysql::Connection * connection;
    std::string datatable;
};

class Partition;

class MySQLBackend : public ThrudocBackend
{
    public:
        MySQLBackend (const std::string & master_hostname,
                      const short master_port,
                      const std::string & slave_hostname,
                      const short slave_port,
                      const std::string & directory_db,
                      const std::string & username,
                      const std::string & password,
                      int max_value_size);

        ~MySQLBackend ();

        std::vector<std::string> getBuckets ();
        std::string get (const std::string & bucket,
                         const std::string & key);
        void put (const std::string & bucket, const std::string & key,
                  const std::string & value);
        void remove (const std::string & bucket, const std::string & key);
        thrudoc::ScanResponse scan (const std::string & bucket,
                                    const std::string & seed, int32_t count);
        std::string admin (const std::string & op, const std::string & data);
        void validate (const std::string & bucket, const std::string * key,
                       const std::string * value);

    protected:

        FindReturn find_and_checkout (const std::string & bucket,
                                      const std::string & key);
        FindReturn find_next_and_checkout
            (const std::string & bucket,
             const std::string & current_databucket);

    private:
        static log4cxx::LoggerPtr logger;

        mysql::ConnectionFactory * connection_factory;
        std::map<std::string, std::set<Partition*, bool(*)(Partition*, Partition*)>* >
            partitions;
        std::string master_hostname;
        short master_port;
        std::string slave_hostname;
        short slave_port;
        std::string directory_db;
        std::string username;
        std::string password;
        int max_value_size;

        FNV32Hashing hashing;

        std::set<Partition*, bool(*)(Partition*, Partition*)> *
            load_partitions (const std::string & bucket);

        FindReturn and_checkout (mysql::Connection * connection,
                                 mysql::PreparedStatement * statement);
        std::string scan_helper (thrudoc::ScanResponse & scan_response,
                                 FindReturn & find_return,
                                 const std::string & offset, int32_t count);
};

#endif /* HAVE_LIBMYSQLCLIENT_R */

#endif
