#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#if HAVE_LIBMYSQLCLIENT_R

#include "mysql_glue.h"
#include <mysql/errmsg.h>

using namespace thrudoc;
using namespace facebook::thrift::concurrency;
using namespace log4cxx;
using namespace mysql;
using namespace std;

// private
LoggerPtr PreparedStatement::logger (Logger::getLogger ("PreparedStatement"));
LoggerPtr Connection::logger (Logger::getLogger ("Connection"));
LoggerPtr ConnectionFactory::logger (Logger::getLogger ("ConnectionFactory"));

LoggerPtr glogger (Logger::getLogger ("glogger"));

BindParams::~BindParams()
{
    delete [] this->params;
}

BindResults::~BindResults()
{
    delete [] this->results;
}

void StringParams::init (const char * str)
{
    this->set_str (str);

    this->params = new MYSQL_BIND[1];
    memset (this->params, 0, sizeof (MYSQL_BIND) * 1);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->str;
    this->params[0].is_null = &this->str_is_null;
    this->params[0].length = &this->str_length;
}

void StringIntParams::init (const char * str, unsigned int i)
{
    this->set_str (str);
    this->set_i (i);

    this->params = new MYSQL_BIND[2];
    memset (this->params, 0, sizeof (MYSQL_BIND) * 2);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->str;
    this->params[0].is_null = &this->str_is_null;
    this->params[0].length = &this->str_length;
    this->params[1].buffer_type = MYSQL_TYPE_LONG;
    this->params[1].buffer = &this->i;
    this->params[1].is_null = 0;
    this->params[1].length = 0;
}

void StringStringParams::init (const char * str1, const char * str2)
{
    this->set_str1 (str1);
    this->set_str2 (str2);

    this->params = new MYSQL_BIND[2];
    memset (this->params, 0, sizeof (MYSQL_BIND) * 2);
    this->params[0].buffer_type = MYSQL_TYPE_STRING;
    this->params[0].buffer = this->str1;
    this->params[0].is_null = &this->str1_is_null;
    this->params[0].length = &this->str1_length;
    this->params[1].buffer_type = MYSQL_TYPE_STRING;
    this->params[1].buffer = this->str2;
    this->params[1].is_null = &this->str2_is_null;
    this->params[1].length = &this->str2_length;
}

PartitionResults::PartitionResults ()
{
    this->results = new MYSQL_BIND[12];
    memset (this->results, 0, sizeof (MYSQL_BIND) * 12);
    this->results[0].buffer_type = MYSQL_TYPE_LONG;
    this->results[0].buffer = &this->id;
    this->results[0].is_null = &this->id_is_null;
    this->results[0].length = &this->id_length;
    this->results[0].error = &this->id_error;
    this->results[1].buffer_type = MYSQL_TYPE_STRING;
    this->results[1].buffer = this->bucket;
    this->results[1].buffer_length = sizeof (this->bucket);
    this->results[1].is_null = &this->bucket_is_null;
    this->results[1].length = &this->bucket_length;
    this->results[1].error = &this->bucket_error;
    this->results[2].buffer_type = MYSQL_TYPE_DOUBLE;
    this->results[2].buffer = &this->start;
    this->results[2].is_null = &this->start_is_null;
    this->results[2].length = &this->start_length;
    this->results[2].error = &this->start_error;
    this->results[3].buffer_type = MYSQL_TYPE_DOUBLE;
    this->results[3].buffer = &this->end;
    this->results[3].is_null = &this->end_is_null;
    this->results[3].length = &this->end_length;
    this->results[3].error = &this->end_error;
    this->results[4].buffer_type = MYSQL_TYPE_STRING;
    this->results[4].buffer = this->hostname;
    this->results[4].buffer_length = sizeof (this->hostname);
    this->results[4].is_null = &this->hostname_is_null;
    this->results[4].length = &this->hostname_length;
    this->results[4].error = &this->hostname_error;
    this->results[5].buffer_type = MYSQL_TYPE_SHORT;
    this->results[5].buffer = &this->port;
    this->results[5].is_null = &this->port_is_null;
    this->results[5].length = &this->port_length;
    this->results[5].error = &this->port_error;
    this->results[6].buffer_type = MYSQL_TYPE_STRING;
    this->results[6].buffer = this->slave_hostname;
    this->results[6].buffer_length = sizeof (this->slave_hostname);
    this->results[6].is_null = &this->slave_hostname_is_null;
    this->results[6].length = &this->slave_hostname_length;
    this->results[6].error = &this->slave_hostname_error;
    this->results[7].buffer_type = MYSQL_TYPE_SHORT;
    this->results[7].buffer = &this->slave_port;
    this->results[7].is_null = &this->slave_port_is_null;
    this->results[7].length = &this->slave_port_length;
    this->results[7].error = &this->slave_port_error;
    this->results[8].buffer_type = MYSQL_TYPE_STRING;
    this->results[8].buffer = this->db;
    this->results[8].buffer_length = sizeof (this->db);
    this->results[8].is_null = &this->db_is_null;
    this->results[8].length = &this->db_length;
    this->results[8].error = &this->db_error;
    this->results[9].buffer_type = MYSQL_TYPE_STRING;
    this->results[9].buffer = this->datatable;
    this->results[9].buffer_length = sizeof (this->datatable);
    this->results[9].is_null = &this->datatable_is_null;
    this->results[9].length = &this->datatable_length;
    this->results[9].error = &this->datatable_error;
    this->results[10].buffer_type = MYSQL_TYPE_TIMESTAMP;
    this->results[10].buffer = &this->created_at;
    this->results[10].is_null = &this->created_at_is_null;
    this->results[10].length = &this->created_at_length;
    this->results[10].error = &this->created_at_error;
    this->results[11].buffer_type = MYSQL_TYPE_DATETIME;
    this->results[11].buffer = &this->retired_at;
    this->results[11].is_null = &this->retired_at_is_null;
    this->results[11].length = &this->retired_at_length;
    this->results[11].error = &this->retired_at_error;
}

KeyValueResults::KeyValueResults (int max_value_size)
{
    // for the null term char
    max_value_size++;

    this->results = new MYSQL_BIND[4];
    memset (this->results, 0, sizeof (MYSQL_BIND) * 4);
    this->results[0].buffer_type = MYSQL_TYPE_STRING;
    this->results[0].buffer = this->key;
    this->results[0].buffer_length = sizeof (this->key);
    this->results[0].is_null = &this->key_is_null;
    this->results[0].length = &this->key_length;
    this->results[0].error = &this->key_error;
    this->results[1].buffer_type = MYSQL_TYPE_STRING;
    this->value = (char *)malloc (max_value_size * sizeof (char));
    this->results[1].buffer = this->value;
    this->results[1].buffer_length = max_value_size * sizeof (char);
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

KeyValueResults::~KeyValueResults ()
{
    free (this->value);
}

PreparedStatement::PreparedStatement (Connection * connection,
                                      const char * query, bool writes,
                                      BindParams * bind_params)
{
    init (connection, query, writes, bind_params, NULL);
}

PreparedStatement::PreparedStatement (Connection * connection,
                                      const char * query, bool writes,
                                      BindParams * bind_params,
                                      BindResults * bind_results)
{
    init (connection, query, writes, bind_params, bind_results);
}

PreparedStatement::~PreparedStatement ()
{
    if (this->stmt)
    {
        mysql_stmt_close (this->stmt);
        this->stmt = NULL;
    }
    if (this->bind_params)
        delete this->bind_params;
    if (this->bind_results)
        delete this->bind_results;
}

void PreparedStatement::init (Connection * connection, const char * query,
                              bool writes, BindParams * bind_params,
                              BindResults * bind_results)
{
    this->connection = connection;
    this->query = query;
    this->writes = writes;
    this->bind_params = bind_params;
    this->bind_results = bind_results;
    LOG4CXX_DEBUG (logger, string ("init: query=") + this->query);

    MYSQL * mysql = this->connection->get_mysql ();
    this->stmt = mysql_stmt_init (mysql);
    if (this->stmt == NULL)
    {
        char buf[1024];
        sprintf (buf, "mysql_stmt_init failed: %p - %d - %s - %s", this->stmt,
                 mysql_errno (mysql), mysql_error (mysql), query);
        LOG4CXX_ERROR (logger, buf);
        ThrudocException e;
        e.what = "MySQLBackend error";
        throw e;
    }

    int ret;
    if ((ret = mysql_stmt_prepare (this->stmt, this->query,
                                   strlen (this->query))) != 0)
    {
        int err = mysql_stmt_errno (this->stmt);
        char buf[1024];
        sprintf (buf, "mysql_stmt_prepare failed: %d - %p - %d - %s - %s",
                 ret, this->stmt, err, mysql_stmt_error (this->stmt), query);
        LOG4CXX_ERROR (logger, buf);

        // NOTE: the first two are what i've actually been seeing, the second
        // two are what the docs say can be returned :(
        if (err == CR_CONN_HOST_ERROR || err == CR_CONNECTION_ERROR ||
            err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST)
        {
            this->connection->lost_connection ();
        }

        // this prepared statement object hasn't been returned and associated
        // with the connection object yet so it can't have been deleted and
        // cleaned up as the others are in lost_connection (). at this point
        // we "own" bind_params and bind_results so we need to get rid of them
        // i don't really like this, but it's necessary. might be a sign that
        // something's not quite right about how my prepared statement stuff
        // is laid out.
        if (bind_params)
            delete bind_params;
        if (bind_results)
            delete bind_results;

        ThrudocException e;
        e.what = "MySQLBackend error";
        throw e;
    }

    if (this->bind_params != NULL)
    {
        if (mysql_stmt_bind_param (this->stmt,
                                   this->bind_params->get_params ()))
        {
            LOG4CXX_ERROR (logger, "mysql_stmt_bind_param failed");
            ThrudocException e;
            e.what = "MySQLBackend error";
            throw e;
        }
    }

    if (this->bind_results)
    {
        if (mysql_stmt_bind_result (this->stmt,
                                    this->bind_results->get_results ()))
        {
            char buf[1024];
            sprintf (buf, "mysql_stmt_bind_result failed: %p - %d - %s",
                     this->stmt, mysql_stmt_errno (this->stmt),
                     mysql_stmt_error (this->stmt));
            LOG4CXX_ERROR (logger, buf);
            ThrudocException e;
            e.what = "MySQLBackend error";
            throw e;
        }
    }
}

void PreparedStatement::execute ()
{
    LOG4CXX_DEBUG (logger, "execute");

    // if this statements writes and we're a read only connection, fail

    if (this->connection->get_read_only ())
    {
        if (this->writes)
        {
            ThrudocException e;
            e.what = "MySQLBackend read-only error";
            LOG4CXX_DEBUG (logger, "execute error: what=" + e.what);
            throw e;
        }
    }

    int ret;
    if ((ret = mysql_stmt_execute (this->stmt)) != 0)
    {
        int err = mysql_stmt_errno (this->stmt);
        char buf[1024];
        sprintf (buf, "mysql_stmt_execute failed: %d - %p - %d - %s", ret,
                 this->stmt, err, mysql_stmt_error (this->stmt));
        LOG4CXX_ERROR (logger, buf);

        // NOTE: the first two are what i've actually been seeing, the second
        // two are what the docs say can be returned :(
        if (err == CR_CONN_HOST_ERROR || err == CR_CONNECTION_ERROR ||
            err == CR_SERVER_GONE_ERROR || err == CR_SERVER_LOST)
        {
            this->connection->lost_connection ();
        }

        ThrudocException e;
        e.what = "MySQLBackend error";
        throw e;
    }

    // optional, buffer results to client
    if ((ret = mysql_stmt_store_result (this->stmt)) != 0)
    {
        char buf[1024];
        sprintf (buf, "mysql_stmt_store_result failed: %d - %p - %d - %s", ret,
                 this->stmt, mysql_stmt_errno (this->stmt),
                 mysql_stmt_error (this->stmt));
        LOG4CXX_ERROR (logger, buf);
        ThrudocException e;
        e.what = "MySQLBackend error";
        throw e;
    }
}

void Connection::reset_connection ()
{
    LOG4CXX_DEBUG (logger, "reset_connection");

    // TODO: figure out how to get .clear () to do the deletes for us if
    // possible?

    // get rid of all of it's prepared statements, we'll reconnect,
    // but they'll be bad now
    map<string, PreparedStatement *>::iterator i;
    for (i = partitions_statements.begin ();
         i != partitions_statements.end (); i++)
        delete i->second;
    partitions_statements.clear ();
    for (i = next_statements.begin (); i != next_statements.end (); i++)
        delete i->second;
    next_statements.clear ();
    for (i = get_statements.begin (); i != get_statements.end (); i++)
        delete i->second;
    get_statements.clear ();
    for (i = put_statements.begin (); i != put_statements.end (); i++)
        delete i->second;
    put_statements.clear ();
    for (i = delete_statements.begin (); i != delete_statements.end (); i++)
        delete i->second;
    delete_statements.clear ();
    for (i = scan_statements.begin (); i != scan_statements.end (); i++)
        delete i->second;
    scan_statements.clear ();
}

void Connection::switch_to_master ()
{
    LOG4CXX_INFO (logger, "switch_to_master");
    // we're in read only
    if (this->read_only)
    {
        // we're working with slave
        reset_connection ();
        this->read_only = 0;
    }
}

void Connection::switch_to_slave ()
{
    LOG4CXX_INFO (logger, "switch_to_slave");
    // we're not already read_only and we have a slave
    if (!this->read_only && !this->slave_hostname.empty ())
    {
        // we're working with slave
        reset_connection ();
        LOG4CXX_DEBUG (logger, "switch_to_slave: setting read_only");
        this->read_only = time (NULL) + MYSQL_MASTER_RETRY_WAIT;
    }
}

void Connection::check_master ()
{
    LOG4CXX_DEBUG (logger, "check_master");
    // if it's time to check the master again
    if (this->read_only && this->read_only < time (0))
    {
        LOG4CXX_DEBUG (logger, "check_master: checking");
        // if we can talk to the master again
        if (mysql_ping (&this->mysql) == 0)
        {
            switch_to_master ();
        }
        else
        {
            LOG4CXX_DEBUG (logger, "check_master: failed, resetting read_only");
            this->read_only = time (NULL) + MYSQL_MASTER_RETRY_WAIT;
        }
    }
}

void Connection::lost_connection ()
{
    LOG4CXX_INFO (logger, "lost_connection");

    // if we lost the connection, we always have to reset
    reset_connection ();

    // if we're not in read_only mode already
    if (!this->read_only)
    {
        // see if we're connected to the master, (auto-reconnected at play
        // here) avoids switching to master when it was just a broken
        // connection, ie the master is still there.
        if (mysql_ping (&this->mysql) != 0)
        {
            // if not then switch to read only mode if we can
            if (!this->slave_hostname.empty ())
            {
                switch_to_slave ();
            }
        }
    }
}

unsigned long PreparedStatement::num_rows ()
{
    LOG4CXX_DEBUG (logger, "execute");

    mysql_stmt_store_result (this->stmt);
    return mysql_stmt_num_rows (this->stmt);
}

int PreparedStatement::fetch ()
{
    LOG4CXX_DEBUG (logger, "fetch");
    int ret = mysql_stmt_fetch (this->stmt);
    if (ret != 0 && ret != MYSQL_NO_DATA)
    {
        char buf[1024];
        sprintf (buf, "mysql_stmt_fetch failed: %d - %p - %d - %s",
                 ret, this->stmt, mysql_stmt_errno (this->stmt),
                 mysql_stmt_error (this->stmt));
        LOG4CXX_ERROR (logger,buf);
        ThrudocException e;
        e.what = "MySQLBackend error";
        throw e;
    }
    return ret;
}

void PreparedStatement::free_result ()
{
    LOG4CXX_DEBUG (logger, "free_result");
    int ret = mysql_stmt_free_result (this->stmt);
    if (ret != 0)
    {
        char buf[1024];
        sprintf (buf, "mysql_stmt_free_result failed: %d - %p - %d - %s",
                 ret, this->stmt, mysql_stmt_errno (this->stmt),
                 mysql_stmt_error (this->stmt));
        LOG4CXX_ERROR (logger,buf);
    }
}

Connection::Connection (const char * hostname, const short port,
                        const char * slave_hostname, const short slave_port,
                        const char * db, const char * username,
                        const char * password)
{
    this->hostname = hostname;
    this->port = port;
    this->db = db;
    this->read_only = 0;

    LOG4CXX_DEBUG (logger, string ("Connection: setting up master hostname=") +
                   hostname);

    if (!mysql_init (&this->mysql))
        LOG4CXX_ERROR (logger, "mysql_init master failed");

    my_bool val = true;
    mysql_options (&this->mysql, MYSQL_OPT_RECONNECT, &val);

    if (!mysql_real_connect (&this->mysql, hostname, username, password, db,
                             port, NULL, 0))
        LOG4CXX_ERROR (logger,
                       string ("mysql_real_connect master failed: host=") +
                       string (hostname) + string (", db=") + string (db) +
                       string (", username=") + string (username));

    // doing this before and after to work around potential bugs in mysql
    // server versions < 5.0.19
    mysql_options (&this->mysql, MYSQL_OPT_RECONNECT, &val);

    if (slave_hostname && strlen (slave_hostname) > 0)
    {
        this->slave_hostname = slave_hostname;
        this->slave_port = slave_port;

        LOG4CXX_DEBUG (logger,
                       string ("Connection: setting up slave hostname=") +
                       slave_hostname);

        if (!mysql_init (&this->slave_mysql))
            LOG4CXX_ERROR (logger, "mysql_init slave failed");

        my_bool val = true;
        mysql_options (&this->slave_mysql, MYSQL_OPT_RECONNECT, &val);

        if (!mysql_real_connect (&this->slave_mysql, slave_hostname,
                                 username, password, db, slave_port, NULL, 0))
            LOG4CXX_ERROR (logger,
                           string ("mysql_real_connect slave failed: host=") +
                           string (hostname) + string (", db=") + string (db) +
                           string (", username=") + string (username));

        // doing this before and after to work around potential bugs in mysql
        // server versions < 5.0.19
        mysql_options (&this->slave_mysql, MYSQL_OPT_RECONNECT, &val);
    }
}

Connection::~Connection ()
{
    LOG4CXX_DEBUG (logger, "~Connection");
    reset_connection ();
    mysql_close (&this->mysql);
}

PreparedStatement * Connection::find_partitions_statement ()
{
    string key = "directory";
    PreparedStatement * stmt = this->partitions_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringParams ();
        BindResults * bind_results = new PartitionResults ();
        const char query[] = "select d.id, bucket, start, end, h.hostname, h.port, s.hostname, s.port, db, datatable, created_at, retired_at from directory d join host h on d.host_id = h.id left join host s on h.slave_id = s.id where bucket = ? and retired_at is null order by end asc";
        stmt = new PreparedStatement (this, query, false,
                                      bind_params, bind_results);
        this->partitions_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_next_statement ()
{
    string key = "directory";
    PreparedStatement * stmt = this->next_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringStringParams ();
        BindResults * bind_results = new PartitionResults ();
        const char query[] = "select d.id, bucket, start, end, h.hostname, h.port, s.hostname, s.port, db, datatable, created_at, retired_at from directory d join host h on d.host_id = h.id left join host s on h.slave_id = s.id where bucket = ? and datatable > ? and retired_at is null order by end asc limit 1";
        stmt = new PreparedStatement (this, query, false,
                                      bind_params, bind_results);
        this->next_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_get_statement (const char * bucket,
                                                    int max_value_size)
{
    string key = string (bucket);
    PreparedStatement * stmt = this->get_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringParams ();
        BindResults * bind_results = new KeyValueResults (max_value_size);
        char query[256];
        sprintf (query, "select k, v, created_at, modified_at from %s where k = ?",
                 bucket);
        stmt = new PreparedStatement (this, query, false,
                                      bind_params, bind_results);
        this->get_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_put_statement (const char * bucket)
{
    string key = string (bucket);
    PreparedStatement * stmt = this->put_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringStringParams ();
        char query[256];
        sprintf (query, "insert into %s (k, v, created_at) values (?, ?, now()) on duplicate key update v = values (v)",
                 bucket);
        stmt = new PreparedStatement (this, query, true,
                                      bind_params);
        this->put_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_delete_statement (const char * bucket)
{
    string key = string (bucket);
    PreparedStatement * stmt = this->delete_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringParams ();
        char query[128];
        sprintf (query, "delete from %s where k = ?", bucket);
        stmt = new PreparedStatement (this, query, true,
                                      bind_params);
        this->delete_statements[key] = stmt;
    }
    return stmt;
}

PreparedStatement * Connection::find_scan_statement (const char * bucket,
                                                     int max_value_size)
{
    string key = string (bucket);
    PreparedStatement * stmt = this->scan_statements[key];
    if (!stmt)
    {
        BindParams * bind_params = new StringIntParams ();
        BindResults * bind_results = new KeyValueResults (max_value_size);
        char query[256];
        sprintf (query, "select k, v, created_at, modified_at from %s where k > ? order by k asc limit ?",
                 bucket);
        stmt = new PreparedStatement (this, query, false,
                                      bind_params, bind_results);
        this->scan_statements[key] = stmt;
    }
    return stmt;
}

ConnectionFactory::ConnectionFactory ()
{
    // init the per-thread connections key
    pthread_key_create (&connections_key, NULL);
}

ConnectionFactory::~ConnectionFactory ()
{
    // init the per-thread connections key
    pthread_key_delete (connections_key);
}

Connection * ConnectionFactory::get_connection
(const char * hostname, const short port, const char * slave_hostname,
 const short slave_port, const char * db, const char * username,
 const char * password)
{
    // get our per-thread connections map
    map<string, Connection*> * connections =
        (map<string, Connection*>*) pthread_getspecific(connections_key);

    if (connections == NULL)
    {
        // set up it if it doesn't yet exist
        connections = new map<string, Connection*>();
        // store it
        pthread_setspecific(connections_key, connections);
    }

    string key;
    {
        char buf[200];
        sprintf (buf, "%s:%d:%s:%d:%s", hostname, port,
                 slave_hostname, slave_port, db);
        key = string (buf);
    }

    LOG4CXX_DEBUG (logger, string ("get_connection: key=") + key);
    // get the connection for this host/db
    Connection * connection = (*connections)[key];

    if (!connection)
    {
        LOG4CXX_DEBUG (logger, string ("get_connection create: key=") + key);
        connection = new Connection (hostname, port, slave_hostname,
                                     slave_port, db, username, password);
        (*connections)[key] = connection;
    }
    return connection;
}


#endif /* HAVE_LIBMYSQLCLIENT_R */
