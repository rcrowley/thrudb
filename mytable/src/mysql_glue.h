#ifndef _THRUDOC_MYSQL_GLUE_H_
#define _THRUDOC_MYSQL_GLUE_H_

#include <stdio.h>
#include <string.h>
#include <log4cxx/logger.h>
#include <mysql/mysql.h>
#include <map>
#include <stack>
#include "MyTable.h"

using namespace std;
using namespace mytable;

namespace mysql {
    
    /* TODO: any reason not to make this huge?, i guess it will eat up
     * memory, but the number of these objects is limited by the number
     * of connections and tables (pstmts) */
    #define MAX_VALUE_SIZE 4096

    class BindParams
    {
        public:
            MYSQL_BIND * get_params()
            {
                return this->params;
            }
        protected:
            MYSQL_BIND * params;
    };

    class BindResults
    {
        public:
            MYSQL_BIND * get_results()
            {
                return this->results;
            }
        protected:
            MYSQL_BIND * results;
    };

    class StringParams : public BindParams
    {
        public:
            StringParams()
            {
                init(NULL);
            }

            StringParams(const char * key)
            {
                init (key);
            }

            void set_key (const char * key)
            {
                if (key == NULL)
                {
                    this->key_is_null = 1;
                }
                else
                {
                    this->key_is_null = 0;
                    strncpy (this->key, key, 38);
                    this->key_length = strlen (key);
                }
            }

            const char * get_key ()
            {
                return this->key;
            }

        protected:
            char key[38];
            //MYSQL_TYPE key_type = MYSQL_TYPE_STRING;
            unsigned long key_length;
            my_bool key_is_null;

        private:
            void init (const char * key);
    };

    class StringIntParams : public StringParams
    {
        public:
            StringIntParams()
            {
                init(NULL, 0);
            }

            StringIntParams(const char * key, unsigned int count)
            {
                init (key, count);
            }

            void set_count (unsigned int count)
            {
                this->count = count;
            }

            unsigned int get_count ()
            {
                return this->count;
            }

        protected:
            unsigned int count;
            //MYSQL_TYPE count_type = MYSQL_TYPE_STRING;

        private:
            void init (const char * key, unsigned int count);
    };

    class StringStringParams : public StringParams
    {
        public:
            StringStringParams()
            {
                init(NULL, NULL);
            }

            StringStringParams(const char * key, const char * value)
            {
                init (key, value);
            }

            void set_value (const char * value)
            {
                if (value == NULL)
                {
                    this->value_is_null = 1;
                }
                else
                {
                    this->value_is_null = 0;
                    strncpy (this->value, value, 1024);
                    this->value_length = strlen (value);
                }
            }

            const char * get_value ()
            {
                return this->value;
            }

        protected:
            char value[1024];
            //MYSQL_TYPE value_type = MYSQL_TYPE_STRING;
            unsigned long value_length;
            my_bool value_is_null;

        private:
            void init (const char * key, const char * value);
    };


    class PartitionsResults : public BindResults
    {
        public:
            PartitionsResults();

            unsigned long get_id ()
            {
                return this->id;
            }

            const char * get_start ()
            {
                return this->start;
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

            unsigned int get_est_size ()
            {
                return this->est_size;
            }

            MYSQL_TIME get_created_at ()
            {
                return this->created_at;
            }

            MYSQL_TIME get_retired_at ()
            {
                return this->retired_at;
            }

        protected:
            /* 0 */
            unsigned long id;
            //MYSQL_TYPE id_type = MYSQL_TYPE_LONG;
            unsigned long id_length;
            my_bool id_is_null;
            my_bool id_error;

            /* 1 */
            char start[33];
            //MYSQL_TYPE start_type = MYSQL_TYPE_STRING;
            unsigned long start_length;
            my_bool start_is_null;
            my_bool start_error;

            /* 2 */
            char end[33];
            //MYSQL_TYPE end_type = MYSQL_TYPE_STRING;
            unsigned long end_length;
            my_bool end_is_null;
            my_bool end_error;

            /* 3 */
            char host[128];
            //MYSQL_TYPE host_type = MYSQL_TYPE_STRING;
            unsigned long host_length;
            my_bool host_is_null;
            my_bool host_error;

            /* 4 */
            char db[14];
            //MYSQL_TYPE db_type = MYSQL_TYPE_STRING;
            unsigned long db_length;
            my_bool db_is_null;
            my_bool db_error;

            /* 5 */
            char table[14];
            //MYSQL_TYPE table_type = MYSQL_TYPE_STRING;
            unsigned long table_length;
            my_bool table_is_null;
            my_bool table_error;

            /* 6 */
            unsigned int est_size;
            //MYSQL_TYPE est_size_type = MYSQL_TYPE_LONG;
            unsigned long est_size_length;
            my_bool est_size_is_null;
            my_bool est_size_error;

            /* 7 */
            MYSQL_TIME created_at;
            //MYSQL_TYPE created_at_type = MYSQL_TYPE_TIMESTAMP;
            unsigned long created_at_length;
            my_bool created_at_is_null;
            my_bool created_at_error;

            /* 8 */
            MYSQL_TIME retired_at;
            //MYSQL_TYPE retired_at_type = MYSQL_TYPE_DATETIME;
            unsigned long retired_at_length;
            my_bool retired_at_is_null;
            my_bool retired_at_error;
    };

    class KeyValueResults : public BindResults
    {
        public:
            KeyValueResults();

            const char * get_key ()
            {
                return this->key;
            }

            const char * get_value()
            {
                return this->value;
            }

            MYSQL_TIME get_created_at ()
            {
                return this->created_at;
            }

            MYSQL_TIME get_modified_at ()
            {
                return this->modified_at;
            }

        protected:
            /* 0 */
            char key[37];
            //MYSQL_TYPE key_type = MYSQL_TYPE_STRING;
            unsigned long key_length;
            my_bool key_is_null;
            my_bool key_error;

            /* 1 */
            char value[MAX_VALUE_SIZE];
            //MYSQL_TYPE value_type = MYSQL_TYPE_STRING;
            unsigned long value_length;
            my_bool value_is_null;
            my_bool value_error;

            /* 2 */
            MYSQL_TIME created_at;
            //MYSQL_TYPE created_at_type = MYSQL_TYPE_TIMESTAMP;
            unsigned long created_at_length;
            my_bool created_at_is_null;
            my_bool created_at_error;

            /* 3 */
            MYSQL_TIME modified_at;
            //MYSQL_TYPE modified_at_type = MYSQL_TYPE_DATETIME;
            unsigned long modified_at_length;
            my_bool modified_at_is_null;
            my_bool modified_at_error;
    };


    class PreparedStatement
    {
        public:
            PreparedStatement(MYSQL * mysql,
                              const char * query,
                              BindParams * bind_params);

            PreparedStatement(MYSQL * mysql,
                              const char * query,
                              BindParams * bind_params, 
                              BindResults * bind_results);

            MYSQL_STMT * get_stmt ()
            {
                return this->stmt;
            }

            BindParams * get_bind_params ()
            {
                return this->bind_params;
            }

            BindResults * get_bind_results ()
            {
                return this->bind_results;
            }

            void execute ();

            unsigned long num_rows ();

            int fetch ();

        protected:
            const char * query;
            MYSQL_STMT * stmt;
            BindParams * bind_params;
            BindResults * bind_results;

        private:
            static log4cxx::LoggerPtr logger;

            void init(MYSQL * mysql, const char * query,
                      BindParams * bind_params, BindResults * bind_results);
    };

    class Connection
    {
        public:
            static Connection * checkout (const char * hostname, 
                                          const char * db, 
                                          const char * username,
                                          const char * password);
            static void checkin (Connection * connection);

            bool is_same (const char * hostname, const char * db);

            PreparedStatement * find_next_statement (const char * tablename);
            PreparedStatement * find_find_statement (const char * tablename);
            PreparedStatement * find_get_statement (const char * tablename);
            PreparedStatement * find_put_statement (const char * tablename);
            PreparedStatement * find_delete_statement (const char * tablename);
            PreparedStatement * find_scan_statement (const char * tablename);

        protected:
            static map<string, stack<Connection *>*> connections;
            static log4cxx::LoggerPtr logger;

            string hostname;
            string db;
            MYSQL mysql;
            map<string, PreparedStatement *> next_statements;
            map<string, PreparedStatement *> find_statements;
            map<string, PreparedStatement *> get_statements;
            map<string, PreparedStatement *> put_statements;
            map<string, PreparedStatement *> delete_statements;
            map<string, PreparedStatement *> scan_statements;

            Connection(const char * host, const char * db, 
                       const char * username, const char * password);
    };
};


#endif
