#ifndef _THRUDOC_MYSQL_GLUE_H_
#define _THRUDOC_MYSQL_GLUE_H_

#if HAVE_LIBMYSQLCLIENT_R

#include <stdio.h>
#include <string.h>
#include <log4cxx/logger.h>
#include <mysql/mysql.h>
#include <map>
#include <stack>
#include <thrift/concurrency/Mutex.h>
#include "DistStore.h"

using namespace std;
using namespace diststore;

namespace mysql {

    #define MYSQL_BACKEND_MAX_STRING_SIZE 64

    #define MYSQL_BACKEND_MAX_TABLENAME_SIZE 32
    #define MYSQL_BACKEND_MAX_HOST_SIZE 128
    #define MYSQL_BACKEND_MAX_DB_SIZE 14
    #define MYSQL_BACKEND_MAX_DATATABLE_SIZE 14
    #define MYSQL_BACKEND_MAX_KEY_SIZE 64

    class BindParams
    {
        public:
            MYSQL_BIND * get_params ()
            {
                return this->params;
            }
        protected:
            MYSQL_BIND * params;
    };

    class BindResults
    {
        public:
            MYSQL_BIND * get_results ()
            {
                return this->results;
            }
        protected:
            MYSQL_BIND * results;
    };

    class StringParams : public BindParams
    {
        public:
            StringParams ()
            {
                init (NULL);
            }

            StringParams (const char * str)
            {
                init (str);
            }

            void set_str (const char * str)
            {
                if (str == NULL)
                {
                    this->str_is_null = 1;
                }
                else
                {
                    this->str_is_null = 0;
                    strncpy (this->str, str, sizeof (this->str));
                    this->str_length = strlen (str);
                }
            }

            const char * get_str ()
            {
                return this->str;
            }

        protected:
            char str[MYSQL_BACKEND_MAX_STRING_SIZE];
            //MYSQL_TYPE str_type = MYSQL_TYPE_STRING;
            unsigned long str_length;
            my_bool str_is_null;

        private:
            void init (const char * str);
    };

    class StringIntParams : public BindParams
    {
        public:
            StringIntParams ()
            {
                init (NULL, 0);
            }

            StringIntParams (const char * str, unsigned int i)
            {
                init (str, i);
            }

            void set_str (const char * str)
            {
                if (str == NULL)
                {
                    this->str_is_null = 1;
                }
                else
                {
                    this->str_is_null = 0;
                    strncpy (this->str, str, sizeof (this->str));
                    this->str_length = strlen (str);
                }
            }

            const char * get_str ()
            {
                return this->str;
            }


            void set_i (unsigned int i)
            {
                this->i = i;
            }

            unsigned int get_i ()
            {
                return this->i;
            }

        protected:
            char str[MYSQL_BACKEND_MAX_STRING_SIZE];
            //MYSQL_TYPE str_type = MYSQL_TYPE_STRING;
            unsigned long str_length;
            my_bool str_is_null;

            unsigned int i;
            //MYSQL_TYPE i_type = MYSQL_TYPE_STRING;

        private:
            void init (const char * str, unsigned int i);
    };

    class StringStringParams : public BindParams
    {
        public:
            StringStringParams ()
            {
                init (NULL, NULL);
            }

            StringStringParams (const char * str1, const char * str2)
            {
                init (str1, str2);
            }

            void set_str1 (const char * str1)
            {
                if (str1 == NULL)
                {
                    this->str1_is_null = 1;
                }
                else
                {
                    this->str1_is_null = 0;
                    strncpy (this->str1, str1, sizeof (this->str1));
                    this->str1_length = strlen (str1);
                }
            }

            const char * get_str1 ()
            {
                return this->str1;
            }

            void set_str2 (const char * str2)
            {
                if (str2 == NULL)
                {
                    this->str2_is_null = 1;
                }
                else
                {
                    this->str2_is_null = 0;
                    strncpy (this->str2, str2, sizeof (this->str2));
                    this->str2_length = strlen (str2);
                }
            }

            const char * get_str2 ()
            {
                return this->str2;
            }

        protected:
            char str1[MYSQL_BACKEND_MAX_STRING_SIZE];
            //MYSQL_TYPE str1_type = MYSQL_TYPE_STRING;
            unsigned long str1_length;
            my_bool str1_is_null;

            char str2[MYSQL_BACKEND_MAX_STRING_SIZE];
            //MYSQL_TYPE str2_type = MYSQL_TYPE_STRING;
            unsigned long str2_length;
            my_bool str2_is_null;

        private:
            void init (const char * str1, const char * str2);
    };


    class PartitionsResults : public BindResults
    {
        public:
            PartitionsResults ();

            unsigned long get_id ()
            {
                return this->id;
            }

            const char * get_tablename ()
            {
                return this->tablename;
            }

            double get_start ()
            {
                return this->start;
            }

            double get_end ()
            {
                return this->end;
            }

            const char * get_host ()
            {
                return this->host;
            }

            const int get_port ()
            {
                return this->port;
            }

            const char * get_db ()
            {
                return this->db;
            }

            const char * get_datatable ()
            {
                return this->datatable;
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
            char tablename[MYSQL_BACKEND_MAX_TABLENAME_SIZE + 1];
            //MYSQL_TYPE tablename_type = MYSQL_TYPE_STRING;
            unsigned long tablename_length;
            my_bool tablename_is_null;
            my_bool tablename_error;

            /* 2 */
            double start;
            //MYSQL_TYPE start_type = MYSQL_TYPE_DOUBLE;
            unsigned long start_length;
            my_bool start_is_null;
            my_bool start_error;

            /* 3 */
            double end;
            //MYSQL_TYPE end_type = MYSQL_TYPE_DOUBLE;
            unsigned long end_length;
            my_bool end_is_null;
            my_bool end_error;

            /* 4 */
            char host[MYSQL_BACKEND_MAX_HOST_SIZE + 1];
            //MYSQL_TYPE host_type = MYSQL_TYPE_STRING;
            unsigned long host_length;
            my_bool host_is_null;
            my_bool host_error;

            /* 5 */
            unsigned short port;
            //MYSQL_TYPE id_type = MYSQL_TYPE_SHORT;
            unsigned long port_length;
            my_bool port_is_null;
            my_bool port_error;

            /* 6 */
            char db[MYSQL_BACKEND_MAX_DB_SIZE + 1];
            //MYSQL_TYPE db_type = MYSQL_TYPE_STRING;
            unsigned long db_length;
            my_bool db_is_null;
            my_bool db_error;

            /* 7 */
            char datatable[MYSQL_BACKEND_MAX_DATATABLE_SIZE + 1];
            //MYSQL_TYPE datatable_type = MYSQL_TYPE_STRING;
            unsigned long datatable_length;
            my_bool datatable_is_null;
            my_bool datatable_error;

            /* 8 */
            MYSQL_TIME created_at;
            //MYSQL_TYPE created_at_type = MYSQL_TYPE_TIMESTAMP;
            unsigned long created_at_length;
            my_bool created_at_is_null;
            my_bool created_at_error;

            /* 9 */
            MYSQL_TIME retired_at;
            //MYSQL_TYPE retired_at_type = MYSQL_TYPE_DATETIME;
            unsigned long retired_at_length;
            my_bool retired_at_is_null;
            my_bool retired_at_error;
    };

    class KeyValueResults : public BindResults
    {
        public:
            KeyValueResults (int max_value_size);

            ~KeyValueResults ();

            const char * get_key ()
            {
                return this->key;
            }

            const char * get_value ()
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
            char key[MYSQL_BACKEND_MAX_KEY_SIZE + 1];
            //MYSQL_TYPE key_type = MYSQL_TYPE_STRING;
            unsigned long key_length;
            my_bool key_is_null;
            my_bool key_error;

            /* 1 */
            char * value;
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
            PreparedStatement (MYSQL * mysql,
                               const char * query,
                               BindParams * bind_params);

            PreparedStatement (MYSQL * mysql,
                               const char * query,
                               BindParams * bind_params,
                               BindResults * bind_results);

            ~PreparedStatement ();

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

            void free_result ();

        protected:
            const char * query;
            MYSQL_STMT * stmt;
            BindParams * bind_params;
            BindResults * bind_results;

        private:
            static log4cxx::LoggerPtr logger;

            void init (MYSQL * mysql, const char * query,
                       BindParams * bind_params, BindResults * bind_results);
    };

    class Connection
    {
        public:
            Connection (const char * host, const char * db, const int port,
                        const char * username, const char * password);
            ~Connection ();

            PreparedStatement * find_partitions_statement (const char * tablename);
            PreparedStatement * find_next_statement (const char * tablename);
            PreparedStatement * find_get_statement (const char * tablename,
                                                    int max_value_size);
            PreparedStatement * find_put_statement (const char * tablename);
            PreparedStatement * find_delete_statement (const char * tablename);
            PreparedStatement * find_scan_statement (const char * tablename,
                                                     int max_value_size);

            string get_hostname ()
            {
                return this->hostname;
            }

            short get_port ()
            {
                return this->port;
            }

            string get_db ()
            {
                return this->db;
            }

        protected:
            static log4cxx::LoggerPtr logger;

            string hostname;
            int port;
            string db;
            MYSQL mysql;
            map<string, PreparedStatement *> partitions_statements;
            map<string, PreparedStatement *> next_statements;
            map<string, PreparedStatement *> get_statements;
            map<string, PreparedStatement *> put_statements;
            map<string, PreparedStatement *> delete_statements;
            map<string, PreparedStatement *> scan_statements;

    };
};

#endif /* HAVE_LIBMYSQLCLIENT_R */

#endif
