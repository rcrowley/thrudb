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
    LOG4CXX_ERROR (logger, "MySQLBackend ()");
    master_hostname = ConfigManager->read<string> ("MYSQL_MASTER_HOSTNAME", 
                                                   "localhost");
    LOG4CXX_ERROR (logger, string ("master_hostname=") + master_hostname);
    master_port = ConfigManager->read<int> ("MYSQL_MASTER_PORT", 3306);
    char buf[64];
    sprintf (buf, "master_port=%d\n", master_port);
    LOG4CXX_ERROR (logger, buf);
    master_db = ConfigManager->read<string> ("MYSQL_MASTER_DB", "mytable");
    LOG4CXX_ERROR (logger, string ("master_db=") + master_db);
    master_username = ConfigManager->read<string> ("MYSQL_MASTER_USERNAME",
                                                   "mytable");
    LOG4CXX_ERROR (logger, string ("master_username=") + master_username);
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
        LOG4CXX_ERROR(logger,buf);
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
                                  FindReturn & find_return, string & offset, 
                                  int32_t count)
{
    Connection * connection = find_return.connection;

    PreparedStatement * scan_statement = 
        connection->find_scan_statement (find_return.data_tablename.c_str ());

    KeyCountParams * kcp = (KeyCountParams*)scan_statement->get_bind_params ();
    kcp->set_key (offset.c_str ());
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
        LOG4CXX_ERROR(logger, buf);
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

    string offset;
    if (seed == "")
    {
        // TODO: this should be a key in the lowest partition...
        offset = "0";
    }
    else
    {
        offset = seed;
        LOG4CXX_ERROR (logger, string ("offset=") + offset);
    }

    ScanResponse scan_response;

    string last;

more:
    FindReturn find_return = this->find_and_checkout (tablename, offset);
    last = scan_helper (scan_response, find_return, offset, count);
    this->checkin (find_return.connection);
    // shift to the next offset, partition
    offset = offset;

    if (scan_response.elements.size () < (unsigned int)count)
    {
    }

    scan_response.seed = last;

    // TODO: error handling

    return scan_response;
}

FindReturn MySQLBackend::find_and_checkout (const string & tablename, 
                                            const string & key)
{
    Connection * connection = Connection::checkout ("localhost", "mytable2");

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
    LOG4CXX_ERROR(logger, string ("key=") + key + string (" -> md5key=") +
                  md5key);

    KeyParams * fpp = (KeyParams*)find_statement->get_bind_params ();
    fpp->set_key (md5key.c_str ());

    find_statement->execute ();

    if (find_statement->num_rows () != 1)
    {
        // TODO: error
        LOG4CXX_ERROR(logger, "we didn't get back 1 row");
    }

    bool same_connection = false;
    int ret = find_statement->fetch ();
    FindReturn find_return;
    if (ret == 0)
    {
        PartitionsResults * fpr = 
            (PartitionsResults*)find_statement->get_bind_results ();

        same_connection = connection->is_same (fpr->get_host (), 
                                               fpr->get_db ());
        if (same_connection)
        {
            find_return.connection = connection;
        }
        else
        {
            find_return.connection = Connection::checkout (fpr->get_host (),
                                                           fpr->get_db ());
        }
        find_return.data_tablename = fpr->get_tbl ();

        LOG4CXX_ERROR(logger, string ("data_tablename=") + 
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
        LOG4CXX_ERROR(logger,buf);
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
