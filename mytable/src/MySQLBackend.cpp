#include <openssl/md5.h>
#include "MySQLBackend.h"
#include "ConfigFile.h"

LoggerPtr MySQLBackend::logger (Logger::getLogger ("MySQLBackend"));

string MySQLBackend::master_hostname;
int MySQLBackend::master_port;
string MySQLBackend::master_db;
string MySQLBackend::master_username;
string MySQLBackend::master_password;

MySQLBackend::MySQLBackend ()
{
    LOG4CXX_INFO (logger, "MySQLBackend ()");
    master_hostname = ConfigManager->read<string> ("MYSQL_MASTER_HOSTNAME", 
                                                   "localhost");
    LOG4CXX_INFO (logger, string ("master_hostname=") + master_hostname);
    master_port = ConfigManager->read<int> ("MYSQL_MASTER_PORT", 3306);
    char buf[64];
    sprintf (buf, "master_port=%d\n", master_port);
    LOG4CXX_INFO (logger, buf);
    master_db = ConfigManager->read<string> ("MYSQL_MASTER_DB", "mytable");
    LOG4CXX_INFO (logger, string ("master_db=") + master_db);
    master_username = ConfigManager->read<string> ("MYSQL_MASTER_USERNAME",
                                                   "mytable");
    LOG4CXX_INFO (logger, string ("master_username=") + master_username);
    master_password = ConfigManager->read<string> ("MYSQL_MASTER_PASSWORD",
                                                   "mytable");
}

MySQLBackend::~MySQLBackend ()
{
    // TODO:
}

string MySQLBackend::get (const string & tablename, const string & key )
{
    FindReturn find_return = this->find_and_checkout (tablename, key);
    Connection * connection = find_return.connection;

    PreparedStatement * get_statement = 
        connection->find_get_statement(find_return.data_tablename.c_str ());

    KeyParams * tkp = (KeyParams*)get_statement->get_bind_params ();
    tkp->set_key (key.c_str ());

    get_statement->execute ();


    string value;
    int ret = get_statement->fetch ();
    if (ret == 0)
    {
        KeyValueResults * kvr = 
            (KeyValueResults*)get_statement->get_bind_results ();
        char buf[1024];
        sprintf(buf, "result:\n\tkey:        %s\n\tvalue:      %s\n\tcreated_at: %s\n\tmodifed_at: %s", 
                kvr->get_key (),
                kvr->get_value (), "U", "U");
        LOG4CXX_INFO(logger,buf);
        value = kvr->get_value ();
    }
    else if (ret == MYSQL_NO_DATA)
    {
        LOG4CXX_ERROR(logger, "we didn't get k,v data");
        MyTableException e;
        e.what = tablename + " " + key + " not found";
        throw e;
    }
    else
    {
        // TODO: ERROR
    }

    this->checkin (find_return.connection);

    // TODO: this will blow things up if return is null
    return value;
}

void MySQLBackend::put (const string & tablename, const string & key, const string & value)
{
    FindReturn find_return = this->find_and_checkout (tablename, key);
    Connection * connection = find_return.connection;

    PreparedStatement * put_statement = 
        connection->find_put_statement (find_return.data_tablename.c_str ());

    KeyValueParams * kvp = (KeyValueParams*)put_statement->get_bind_params ();
    kvp->set_key (key.c_str ());
    kvp->set_value (value.c_str ());

    put_statement->execute ();

    this->checkin (find_return.connection);
}

void MySQLBackend::remove (const string & tablename, const string & key )
{
    FindReturn find_return = this->find_and_checkout (tablename, key);
    Connection * connection = find_return.connection;

    PreparedStatement * delete_statement = 
        connection->find_delete_statement (find_return.data_tablename.c_str ());

    KeyParams * kvp = (KeyParams*)delete_statement->get_bind_params ();
    kvp->set_key (key.c_str ());

    delete_statement->execute ();

    this->checkin (find_return.connection);
}

string MySQLBackend::scan_helper (ScanResponse & scan_response, 
                                  FindReturn & find_return, 
                                  const string & seed, 
                                  int32_t count)
{
    {
        char buf[1024];
        sprintf (buf, "scan_helper: data_tablename=%s, seed=%s, count=%d", 
                 find_return.data_tablename.c_str (), seed.c_str (), count);
        LOG4CXX_INFO (logger, buf);
    }
    PreparedStatement * scan_statement = 
        find_return.connection->find_scan_statement 
        (find_return.data_tablename.c_str ());

    KeyCountParams * kcp = (KeyCountParams*)scan_statement->get_bind_params ();
    kcp->set_key (seed.c_str ());
    kcp->set_count (count);

    scan_statement->execute();

    int ret;
    KeyValueResults * kvr = 
        (KeyValueResults*)scan_statement->get_bind_results ();
    while ((ret = scan_statement->fetch ()) == 0)
    {
        // we gots results
        Element * e = new Element();
        e->key = kvr->get_key ();
        e->value = kvr->get_value ();
        scan_response.elements.push_back (*e);
    }

    if (ret != MYSQL_NO_DATA)
    {
        char buf[1024];
        sprintf (buf, "ret=%d", ret);
        LOG4CXX_INFO(logger, buf);
    }

    return string (kvr->get_key ());
}

ScanResponse MySQLBackend::scan (const string & tablename, const string & seed,
                                 int32_t count)
{
    // if seed is null, then start with the first partition
    //   this means we'll have to query for the first partition
    // else start with seed's parition
    // ask for count elements
    // if we don't get back count elements, go to the next partition and ask for count - have
    // return elements

    // base data_tablename should be > "0"
    FindReturn find_return;
    if (seed != "")
    {
        find_return = this->find_and_checkout (tablename, seed);
    }
    else
    {
        // first call, get the first data_tablename
        find_return = this->find_next_and_checkout (tablename, "0");
    }
    LOG4CXX_INFO (logger, string ("data_tablename=") + 
                  find_return.data_tablename);

    ScanResponse scan_response;

    int size = 0;
    string offset = seed == "" ? string ("0") : seed;

more:
    offset = this->scan_helper (scan_response, find_return, offset, 
                                count - size);
    size = (int)scan_response.elements.size ();

    if (scan_response.elements.size () < (unsigned int)count)
    {
        find_return = this->find_next_and_checkout (tablename, 
                                                    find_return.data_tablename);
        offset = "0";
        if (find_return.connection != NULL)
        {
            // we have more partitions
            goto more;
        }
    }

    // TODO: error handling

    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";

    return scan_response;
}

FindReturn MySQLBackend::find_and_checkout (const string & tablename, 
                                            const string & key)
{
    Connection * connection = Connection::checkout 
        (this->master_hostname.c_str (), this->master_db.c_str (),
         this->master_username.c_str (), this->master_password.c_str ());

    PreparedStatement * find_statement = 
        connection->find_find_statement (tablename.c_str ());

    // we partition by the md5 of the key so that we'll get an even distrobution 
    // of keys across partitions.
    unsigned char md5[16];
    memset (md5, 0, sizeof (md5));
    MD5((const unsigned char *)key.c_str (), key.length (), md5);
    string md5key;
    char hex[3];
    for (int i = 0; i < 16; i++)
    {
        sprintf (hex, "%02x", md5[i]);
        md5key += string (hex);
    }
    LOG4CXX_INFO(logger, string ("key=") + key + string (" -> md5key=") +
                 md5key);

    KeyParams * fpp = (KeyParams*)find_statement->get_bind_params ();
    fpp->set_key (md5key.c_str ());

    return and_checkout (connection, find_statement);
}

FindReturn MySQLBackend::find_next_and_checkout (const string & tablename, 
                                                 const string & current_data_tablename)
{
    Connection * connection = Connection::checkout 
        (this->master_hostname.c_str (), this->master_db.c_str (),
         this->master_username.c_str (), this->master_password.c_str ());

    PreparedStatement * next_statement = 
        connection->find_next_statement (tablename.c_str ());

    KeyParams * fpp = (KeyParams*)next_statement->get_bind_params ();
    fpp->set_key (current_data_tablename.c_str ());

    return and_checkout (connection, next_statement);
}

FindReturn MySQLBackend::and_checkout (Connection * connection, 
                                       PreparedStatement * statement)
{
    statement->execute ();

    if (statement->num_rows () != 1)
    {
        // TODO: error
        LOG4CXX_ERROR(logger, "we didn't get back 1 row");
    }

    bool same_connection = false;
    int ret = statement->fetch ();
    FindReturn find_return;
    find_return.connection = NULL;
    if (ret == 0)
    {
        PartitionsResults * fpr = 
            (PartitionsResults*)statement->get_bind_results ();

        same_connection = connection->is_same (fpr->get_host (), 
                                               fpr->get_db ());
        if (same_connection)
        {
            find_return.connection = connection;
        }
        else
        {
            find_return.connection = Connection::checkout 
                (fpr->get_host (), fpr->get_db (),
                 this->master_username.c_str (), 
                 this->master_password.c_str ());
        }
        find_return.data_tablename = fpr->get_tbl ();

        LOG4CXX_INFO(logger, string ("data_tablename=") + 
                     find_return.data_tablename);

        char buf[1024];
        sprintf(buf, "result:\n\tid:         %d\n\tstart:      %s\n\tend:        %s\n\thost:       %s\n\tdb:         %s\n\ttbl:        %s\n\test_size:   %d\n\tcreated_at: %s\n\tretired_at: %s", 
                (int)fpr->get_id (),
                fpr->get_start (),
                fpr->get_end (),
                fpr->get_host (),
                fpr->get_db (),
                fpr->get_tbl (),
                fpr->get_est_size (), "U", "U"
                /*
                   !bp.created_at_is_null ? bp.created_at : "nil",
                   !bp.retired_at_is_null ? bp.retired_at : "nil"
                   */
               );
        LOG4CXX_INFO(logger,buf);
    }
    else if (ret == MYSQL_NO_DATA)
    {
        LOG4CXX_ERROR(logger, "we didn't get back row data");
    }
    else
    {
        // TODO: ERROR
    }

    // if we're not returning our connection, check it back in
    if (!same_connection)
        Connection::checkin (connection);

    return find_return;
}

void MySQLBackend::checkin (Connection * connection)
{
    Connection::checkin (connection);
}
