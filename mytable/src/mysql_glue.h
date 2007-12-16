#ifndef _THRUDOC_MYSQL_GLUE_H_
#define _THRUDOC_MYSQL_GLUE_H_

#include <stdio.h>
#include <string.h>
#include <log4cxx/logger.h>
#include <mysql/mysql.h>
#include <map>
#include <stack>

using namespace std;

namespace mysql {
    
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

    class KeyParams : public BindParams
    {
        public:
            KeyParams()
            {
                init(NULL);
            }

            KeyParams(const char * key)
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
            my_bool key_error;

        private:
            void init (const char * key);
    };

    class KeyValueParams : public KeyParams
    {
        public:
            KeyValueParams()
            {
                init(NULL, NULL);
            }

            KeyValueParams(const char * key, const char * value)
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
            my_bool value_error;

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

            const char * get_tbl ()
            {
                return this->tbl;
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
            char start[128];
            //MYSQL_TYPE start_type = MYSQL_TYPE_STRING;
            unsigned long start_length;
            my_bool start_is_null;
            my_bool start_error;

            /* 2 */
            char end[128];
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
            char db[128];
            //MYSQL_TYPE db_type = MYSQL_TYPE_STRING;
            unsigned long db_length;
            my_bool db_is_null;
            my_bool db_error;

            /* 5 */
            char tbl[128];
            //MYSQL_TYPE tbl_type = MYSQL_TYPE_STRING;
            unsigned long tbl_length;
            my_bool tbl_is_null;
            my_bool tbl_error;

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
            char value[1024];
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

            int execute ();

            unsigned long num_rows ();

            int fetch ();

        protected:
            const char * query;
            MYSQL_STMT * stmt;
            BindParams * bind_params;
            BindResults * bind_results;

        private:
            void init(MYSQL * mysql, const char * query,
                      BindParams * bind_params, BindResults * bind_results);

            static log4cxx::LoggerPtr logger;
    };

    class Connection
    {
        public:
            static Connection * checkout (const char * hostname, 
                                          const char * db);
            static void checkin (Connection * connection);

            bool is_same (const char * hostname, const char * db);

            PreparedStatement * find_find_statement (const char * tablename);
            PreparedStatement * find_get_statement (const char * tablename);
            PreparedStatement * find_put_statement (const char * tablename);
            PreparedStatement * find_delete_statement (const char * tablename);

        protected:
            static map<string, stack<Connection *>*> connections;
            static log4cxx::LoggerPtr logger;

            string hostname;
            string db;
            MYSQL mysql;
            map<string, PreparedStatement *> find_statements;
            map<string, PreparedStatement *> get_statements;
            map<string, PreparedStatement *> put_statements;
            map<string, PreparedStatement *> delete_statements;

            Connection(const char * host, const char * db);
    };
};


#endif
