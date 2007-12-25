
#include "mysql_glue.h"

using namespace std;
using namespace log4cxx;
using namespace mysql;

LoggerPtr PreparedStatement::logger(Logger::getLogger("PreparedStatement"));
LoggerPtr Connection::logger(Logger::getLogger("Connection"));

void KeyParams::init (const char * key)
{
    this->set_key (key);

    this->params = new MYSQL_BIND[1];
    memset (this->params, 0, sizeof(MYSQL_BIND) * 1);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->key;
    this->params[0].is_null = &this->key_is_null;
    this->params[0].length = &this->key_length;
}

void KeyCountParams::init (const char * key, unsigned int count)
{
    this->set_key (key);
    this->set_count (count);

    this->params = new MYSQL_BIND[2];
    memset (this->params, 0, sizeof(MYSQL_BIND) * 2);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->key;
    this->params[0].is_null = &this->key_is_null;
    this->params[0].length = &this->key_length;
    this->params[1].buffer_type = MYSQL_TYPE_LONG;
    this->params[1].buffer = &this->count;
    this->params[1].is_null = 0;
    this->params[1].length = 0;
}

void KeyValueParams::init (const char * key, const char * value)
{
    this->set_key (key);
    this->set_value (value);

    this->params = new MYSQL_BIND[2];
    memset (this->params, 0, sizeof(MYSQL_BIND) * 2);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->key;
    this->params[0].is_null = &this->key_is_null;
    this->params[0].length = &this->key_length;
    this->params[1].buffer_type = MYSQL_TYPE_STRING;
    this->params[1].buffer = this->value;
    this->params[1].is_null = &this->value_is_null;
    this->params[1].length = &this->value_length;
}

PartitionsResults::PartitionsResults()
{
    this->results = new MYSQL_BIND[9];
    memset (this->results, 0, sizeof (MYSQL_BIND) * 9);
    this->results[0].buffer_type = MYSQL_TYPE_LONG;
    this->results[0].buffer = &this->id;
    this->results[0].is_null = &this->id_is_null;
    this->results[0].length = &this->id_length;
    this->results[0].error = &this->id_error;
    this->results[1].buffer_type = MYSQL_TYPE_STRING;
    this->results[1].buffer = &this->start;
    this->results[1].buffer_length = sizeof (this->start);
    this->results[1].is_null = &this->start_is_null;
    this->results[1].length = &this->start_length;
    this->results[1].error = &this->start_error;
    this->results[2].buffer_type = MYSQL_TYPE_STRING;
    this->results[2].buffer = &this->end;
    this->results[2].buffer_length = sizeof (this->end);
    this->results[2].is_null = &this->end_is_null;
    this->results[2].length = &this->end_length;
    this->results[2].error = &this->end_error;
    this->results[3].buffer_type = MYSQL_TYPE_STRING;
    this->results[3].buffer = &this->host;
    this->results[3].buffer_length = sizeof (this->host);
    this->results[3].is_null = &this->host_is_null;
    this->results[3].length = &this->host_length;
    this->results[3].error = &this->host_error;
    this->results[4].buffer_type = MYSQL_TYPE_STRING;
    this->results[4].buffer = &this->db;
    this->results[4].buffer_length = sizeof (this->db);
    this->results[4].is_null = &this->db_is_null;
    this->results[4].length = &this->db_length;
    this->results[4].error = &this->db_error;
    this->results[5].buffer_type = MYSQL_TYPE_STRING;
    this->results[5].buffer = &this->tbl;
    this->results[5].buffer_length = sizeof (this->tbl);
    this->results[5].is_null = &this->tbl_is_null;
    this->results[5].length = &this->tbl_length;
    this->results[5].error = &this->tbl_error;
    this->results[6].buffer_type = MYSQL_TYPE_LONG;
    this->results[6].buffer = &this->est_size;
    this->results[6].is_null = &this->est_size_is_null;
    this->results[6].length = &this->est_size_length;
    this->results[6].error = &this->est_size_error;
    this->results[7].buffer_type = MYSQL_TYPE_TIMESTAMP;
    this->results[7].buffer = &this->created_at;
    this->results[7].is_null = &this->created_at_is_null;
    this->results[7].length = &this->created_at_length;
    this->results[7].error = &this->created_at_error;
    this->results[8].buffer_type = MYSQL_TYPE_DATETIME;
    this->results[8].buffer = &this->retired_at;
    this->results[8].is_null = &this->retired_at_is_null;
    this->results[8].length = &this->retired_at_length;
    this->results[8].error = &this->retired_at_error;
}

KeyValueResults::KeyValueResults()
{
    this->results = new MYSQL_BIND[4];
    memset (this->results, 0, sizeof (MYSQL_BIND) * 4);
    this->results[0].buffer_type = MYSQL_TYPE_STRING;
    this->results[0].buffer = &this->key;
    this->results[0].buffer_length = sizeof (this->key);
    this->results[0].is_null = &this->key_is_null;
    this->results[0].length = &this->key_length;
    this->results[0].error = &this->key_error;
    this->results[1].buffer_type = MYSQL_TYPE_STRING;
    this->results[1].buffer = &this->value;
    this->results[1].buffer_length = sizeof (this->value);
    this->results[1].is_null = &this->value_is_null;
    this->results[1].length = &this->value_length;
    this->results[1].error = &this->value_error;
    this->results[2].buffer_type = MYSQL_TYPE_TIMESTAMP;
    this->results[2].buffer = &this->created_at;
    this->results[2].is_null = &this->created_at_is_null;
    this->results[2].length = &this->created_at_length;
    this->results[2].error = &this->created_at_error;
    this->results[3].buffer_type = MYSQL_TYPE_DATETIME;
    this->results[3].buffer = &this->modified_at;
    this->results[3].is_null = &this->modified_at_is_null;
    this->results[3].length = &this->modified_at_length;
    this->results[3].error = &this->modified_at_error;
}

PreparedStatement::PreparedStatement(MYSQL * mysql,
                                     const char * query,
                                     BindParams * bind_params)
{
    init(mysql, query, bind_params, NULL);
}

PreparedStatement::PreparedStatement(MYSQL * mysql,
                                     const char * query,
                                     BindParams * bind_params, 
                                     BindResults * bind_results)
{
    init(mysql, query, bind_params, bind_results);
}

void PreparedStatement::init(MYSQL * mysql, const char * query,
                             BindParams * bind_params, 
                             BindResults * bind_results)
{
    this->query = query;
    this->bind_params = bind_params;
    this->bind_results = bind_results;
    LOG4CXX_INFO(logger, string("init: query=") + this->query);

    // TODO: error handling
    this->stmt = mysql_stmt_init (mysql);
    if (this->stmt == NULL)
    {
        LOG4CXX_ERROR(logger, string("mysql_stmt_init failed: ")+string(query));
        char buf[1024];
        sprintf(buf, "*** %p - %d - %s", this->stmt, mysql_errno (mysql), 
                mysql_error (mysql));
        LOG4CXX_ERROR(logger,buf);
    }
    if (mysql_stmt_prepare (this->stmt, this->query, strlen (this->query)))
    {
        LOG4CXX_ERROR(logger, string("mysql_stmt_prepare failed: ")+string(query));
        char buf[1024];
        sprintf(buf, "*** %p - %d - %s", this->stmt, mysql_errno (mysql), 
                mysql_error (mysql));
        LOG4CXX_ERROR(logger,buf);
    }
    if (this->bind_params != NULL)
    {
        if (mysql_stmt_bind_param (this->stmt, 
                                   this->bind_params->get_params ()))
            LOG4CXX_ERROR(logger, "mysql_stmt_bind_param failed");
    }
}

int PreparedStatement::execute ()
{
    LOG4CXX_INFO(logger, "execute");

    // TODO: error handling
    int ret = mysql_stmt_execute (this->stmt);
    if (ret != 0)
    {
        LOG4CXX_ERROR(logger, string("mysql_stmt_execute failed: "));
        char buf[1024];
        sprintf(buf, "*** %p - %d - %s", this->stmt, 
                mysql_stmt_errno (this->stmt), mysql_stmt_error (this->stmt));
        LOG4CXX_ERROR(logger,buf);
    }
    if (this->bind_results)
    {
        if (mysql_stmt_bind_result (this->stmt, 
                                    this->bind_results->get_results ()))
        {
            LOG4CXX_ERROR(logger, string("mysql_stmt_bind_result failed: "));
            char buf[1024];
            sprintf(buf, "*** %p - %d - %s", this->stmt, 
                    mysql_stmt_errno (this->stmt), 
                    mysql_stmt_error (this->stmt));
            LOG4CXX_ERROR(logger,buf);
        }
        // optional, buffer results to client
        mysql_stmt_store_result (this->stmt);
    }
    return ret;
}

unsigned long PreparedStatement::num_rows ()
{
    LOG4CXX_INFO(logger, "execute");

    mysql_stmt_store_result (this->stmt);
    return mysql_stmt_num_rows (this->stmt);
}

int PreparedStatement::fetch ()
{
    LOG4CXX_INFO(logger, "fetch");
    return mysql_stmt_fetch (this->stmt);
}

map<string, stack<Connection *>*> Connection::connections;

Connection * Connection::checkout(const char * hostname, const char * db,
                                  const char * username, const char * password)
{
    string key = string(hostname) + ":" + string (db);
    stack<Connection *> * conns = connections[key];
    if (!conns)
    {
        conns = new stack<Connection*>();
        connections[key] = conns;
    }
    Connection * conn = NULL;
    if (!conns->empty ())
    {
        conn = conns->top ();
        conns->pop ();
    }
    if (!conn)
    {
        conn = new Connection(hostname, db, username, password);
    }
    {
        char buf[1024];
        sprintf (buf, "checkout=0x%p", conn);
        LOG4CXX_INFO(logger, buf);
    }
    return conn;
}

void Connection::checkin (Connection * connection)
{
    {
        char buf[1024];
        sprintf (buf, "checkin=0x%p", connection);
        LOG4CXX_INFO(logger, buf);
    }
    string key = string(connection->hostname) + ":" + string (connection->db);
    stack<Connection *> * conns = connections[key];
    if (!conns)
    {
        // this should never happen
        conns = new stack<Connection*>;
        connections[key] = conns;
    }
    conns->push(connection);
}

Connection::Connection (const char * hostname, const char * db,
                        const char * username, const char * password)
{
    this->hostname = hostname;
    this->db = db;

    if (!mysql_init (&this->mysql))
        LOG4CXX_ERROR(logger, "mysql_init failed");

    if (!mysql_real_connect (&this->mysql, hostname, username, password, db,
                             3306, NULL, 0))
        LOG4CXX_ERROR(logger, string ("mysql_real_connect failed: host=") + 
                      string (hostname) + string (", db=") + string (db) + 
                      string (", username=") + string (username));
}

bool Connection::is_same (const char * hostname, const char * db)
{
    return strcmp (this->hostname.c_str (), hostname) == 0 && 
        strcmp (this->db.c_str (), db) == 0;
}

PreparedStatement * Connection::find_next_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->next_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyParams();
        BindResults * bind_results = new PartitionsResults();
        char query[256];
        sprintf (query, "select id, start, end, host, db, tbl, est_size, created_at, retired_at from %s where tbl > ? and retired_at is null order by end asc limit 1",
                 tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params,
                                     bind_results);
        this->next_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_find_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->find_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyParams();
        BindResults * bind_results = new PartitionsResults();
        char query[256];
        sprintf (query, "select id, start, end, host, db, tbl, est_size, created_at, retired_at from %s where ? <= end and retired_at is null order by end asc limit 1",
                 tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params, 
                                     bind_results);
        this->find_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_get_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->get_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyParams();
        BindResults * bind_results = new KeyValueResults();
        char query[256];
        sprintf (query, "select k, v, created_at, modified_at from %s where k = ?",
                 tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params, 
                                     bind_results);
        this->get_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_put_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->put_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyValueParams();
        char query[256];
        sprintf (query, "insert into %s (k, v, created_at) values (?, ?, now()) on duplicate key update v = values(v)",
                 tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params);
        this->put_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_delete_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->delete_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyParams();
        char query[128];
        sprintf (query, "delete from %s where k = ?", tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params);
        this->delete_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_scan_statement(const char * tablename)
{
    string key = string(tablename);
    PreparedStatement * stmt = this->scan_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new KeyCountParams();
        BindResults * bind_results = new KeyValueResults();
        char query[256];
        sprintf (query, "select k, v, created_at, modified_at from %s where k > ? order by k asc limit ?",
                 tablename);
        stmt = new PreparedStatement(&this->mysql, query, bind_params, 
                                     bind_results);
        this->scan_statements[key] = stmt;
    }
    return stmt;
}

