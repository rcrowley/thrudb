#include <openssl/md5.h>
#include "MySQLBackend.h"
#include "ConfigFile.h"

/*
 * TODO:
 * - timeout the directory info at some interval
 * - cleanly recover from lost/broken connections
 * - look at libmemcached for it's partitioning algoritms
 * - think about straight key parititioning to allow in order scans etc...
 */

// protected
map<string, set<Partition*, bool(*)(Partition*, Partition*)>* > MySQLBackend::partitions;
string MySQLBackend::master_hostname;
int MySQLBackend::master_port;
string MySQLBackend::master_db;
string MySQLBackend::master_username;
string MySQLBackend::master_password;

// private
LoggerPtr MySQLBackend::logger (Logger::getLogger ("MySQLBackend"));
pthread_key_t MySQLBackend::connections_key;

MySQLBackend::MySQLBackend ()
{
    LOG4CXX_INFO (logger, "MySQLBackend ()");
    master_hostname = ConfigManager->read<string> ("MYSQL_MASTER_HOSTNAME",
                                                   "localhost");
    LOG4CXX_INFO (logger, string ("master_hostname=") + master_hostname);
    master_port = ConfigManager->read<int> ("MYSQL_MASTER_PORT", 3306);
    {
        char buf[64];
        sprintf (buf, "master_port=%d\n", master_port);
        LOG4CXX_INFO (logger, buf);
    }
    master_db = ConfigManager->read<string> ("MYSQL_MASTER_DB", "mytable");
    LOG4CXX_INFO (logger, string ("master_db=") + master_db);
    master_username = ConfigManager->read<string> ("MYSQL_USERNAME", "mytable");
    LOG4CXX_INFO (logger, string ("master_username=") + master_username);
    master_password = ConfigManager->read<string> ("MYSQL_PASSWORD", "mytable");
        
    pthread_key_create (&connections_key, NULL);
}

set<Partition*, bool(*)(Partition*, Partition*)> * 
MySQLBackend::load_partitions (const string & tablename)
{
    LOG4CXX_INFO (logger, string ("load_partitions: tablename=") + tablename);

    set<Partition*, bool(*)(Partition*, Partition*)> *
        new_partitions = new set<Partition*, bool(*)(Partition*, Partition*)>
        (Partition::greater);

    Connection * connection = get_connection
        (this->master_hostname.c_str (), this->master_db.c_str ());

    PreparedStatement * partitions_statement =
        connection->find_partitions_statement ("directory");

    StringParams * fp = (StringParams*)partitions_statement->get_bind_params ();
    fp->set_str (tablename.c_str ());

    try
    {
        partitions_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (connection);
        throw e;
    }

    PartitionsResults * pr =
        (PartitionsResults*)partitions_statement->get_bind_results ();

    while (partitions_statement->fetch () != MYSQL_NO_DATA)
    {
        LOG4CXX_INFO (logger, string ("  load_partitions inserting: datatable=") +
                      pr->get_datatable () + string (", end=") + 
                      pr->get_end ());
        new_partitions->insert (new Partition (pr));
    }

    if (new_partitions->size () > 0)
    {
        partitions[tablename] = new_partitions;
    }
    else
    {
        LOG4CXX_WARN (logger, string ("load_partitions: request to load ") + 
                      tablename + string (" with no partitions"));
        delete new_partitions;
        new_partitions = NULL;
    }

    return new_partitions;
}

vector<string> MySQLBackend::getTablenames ()
{
    vector<string> tablenames;
    map<string, set<Partition*, bool(*)(Partition*, Partition*)>* >::iterator i;
    for (i = partitions.begin (); i != partitions.end (); i++)
    {
        tablenames.push_back ((*i).first);
    }
    return tablenames;
}

string MySQLBackend::get (const string & tablename, const string & key )
{
    FindReturn find_return = this->find_and_checkout (tablename, key);

    PreparedStatement * get_statement =
        find_return.connection->find_get_statement
        (find_return.datatable.c_str ());

    StringParams * tkp = (StringParams*)get_statement->get_bind_params ();
    tkp->set_str (key.c_str ());

    try
    {
        get_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (find_return.connection);
        throw e;
    }

    string value;
    if (get_statement->fetch () == MYSQL_NO_DATA)
    {
        MyTableException e;
        e.what = key + " not found in " + tablename;
        LOG4CXX_WARN (logger, string ("get: ") + e.what);
        throw e;
    }

    KeyValueResults * kvr =
        (KeyValueResults*)get_statement->get_bind_results ();
    LOG4CXX_DEBUG (logger, string ("get: key=") + kvr->get_key () +
                   string (", value=") + kvr->get_value ());
    value = kvr->get_value ();

    return value;
}

void MySQLBackend::put (const string & tablename, const string & key, const string & value)
{
    FindReturn find_return = this->find_and_checkout (tablename, key);

    PreparedStatement * put_statement =
        find_return.connection->find_put_statement
        (find_return.datatable.c_str ());

    StringStringParams * kvp = (StringStringParams*)put_statement->get_bind_params ();
    kvp->set_str1 (key.c_str ());
    kvp->set_str2 (value.c_str ());

    try
    {
        put_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (find_return.connection);
        throw e;
    }
}

void MySQLBackend::remove (const string & tablename, const string & key )
{
    FindReturn find_return = this->find_and_checkout (tablename, key);

    PreparedStatement * delete_statement =
        find_return.connection->find_delete_statement
        (find_return.datatable.c_str ());

    StringParams * kvp = (StringParams*)delete_statement->get_bind_params ();
    kvp->set_str (key.c_str ());

    try
    {
        delete_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (find_return.connection);
        throw e;
    }
}

string MySQLBackend::scan_helper (ScanResponse & scan_response,
                                  FindReturn & find_return,
                                  const string & seed,
                                  int32_t count)
{
    PreparedStatement * scan_statement =
        find_return.connection->find_scan_statement
        (find_return.datatable.c_str ());

    StringIntParams * kcp =
        (StringIntParams*)scan_statement->get_bind_params ();
    kcp->set_str (seed.c_str ());
    kcp->set_i (count);

    try
    {
        scan_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (find_return.connection);
        throw e;
    }

    int ret;
    KeyValueResults * kvr =
        (KeyValueResults*)scan_statement->get_bind_results ();
    while ((ret = scan_statement->fetch ()) == 0)
    {
        // we gots results
        Element * e = new Element ();
        e->key = kvr->get_key ();
        e->value = kvr->get_value ();
        scan_response.elements.push_back (*e);
    }
    return scan_response.elements.size() > 0 ? 
        scan_response.elements.back ().key : "";
}

/*
 * our seed will just be the last key returned. that's enough for us to find
 * the partition used last
 */
ScanResponse MySQLBackend::scan (const string & tablename, const string & seed,
                                 int32_t count)
{
    // base datatable should be > "0"
    FindReturn find_return;
    if (seed != "")
    {
        // subsequent call, use the normal find method
        find_return = this->find_and_checkout (tablename, seed);
    }
    else
    {
        // first call, get the first datatable
        find_return = this->find_next_and_checkout (tablename, "0");
    }
    LOG4CXX_DEBUG (logger, 
                   string ("scan: hostname=") + find_return.connection->get_hostname () +
                   string (", db=") + find_return.connection->get_db () +
                   string (", datatable=") + find_return.datatable);

    ScanResponse scan_response;

    int size = 0;
    string offset = seed == "" ? string ("0") : seed;

more:
    // get data from our current find_return (parition) starting with values
    // greater than offset, returning count - size values
    offset = this->scan_helper (scan_response, find_return, offset,
                                count - size);
    // grab the current size of our returned elements
    size = (int)scan_response.elements.size ();

    // if we don't have enough elements
    if (scan_response.elements.size () < (unsigned int)count)
    {
        // try to find the next partition
        find_return = this->find_next_and_checkout (tablename,
                                                    find_return.datatable);
        if (find_return.connection != NULL)
        {
            // we have more partitions
            offset = "0"; // start at the begining of this new parition
            goto more; // goto's are fun :)
        }
    }

    // we're done now, return the last element as the seed so if there's more
    // data we'll know how to get at it
    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";

    return scan_response;
}

FindReturn MySQLBackend::find_and_checkout (const string & tablename,
                                            const string & key)
{
    // we partition by the md5 of the key so that we'll get an even
    // distrobution of keys across partitions, we still store with key tho
    // TODO: is there a better way to do key -> md5 string
    unsigned char md5[16];
    memset (md5, 0, sizeof (md5));
    MD5 ((const unsigned char *)key.c_str (), key.length (), md5);
    string md5key;
    char hex[3];
    for (int i = 0; i < 16; i++)
    {
        sprintf (hex, "%02x", md5[i]);
        md5key += string (hex);
    }
    LOG4CXX_DEBUG (logger, string ("key=") + key + string (" -> md5key=") +
                   md5key);

    FindReturn find_return;

    // look for the partitions set, this god awful mess is b/c std::map []
    // creates elements if they don't exist and find returns the last element
    // if what you're looking for doesn't exists, who the fuck came up with
    // this shit.
    set<Partition*, bool(*)(Partition*, Partition*)> * partitions_set = NULL;
    if (partitions.find (tablename) != partitions.end ())
    {
        partitions_set = partitions[tablename];
    }

    if (partitions_set == NULL)
    {
        // we didn't find it, try loading
        partitions_set = this->load_partitions (tablename);
    }

    if (partitions_set != NULL)
    {
        // we now have the partitions set
        Partition * part = new Partition (md5key);
        // look for the matching partition
        set<Partition*>::iterator partition = partitions_set->lower_bound (part);
        // TODO how do we check if we got something "valid" back
        if (*partition)
        {
            LOG4CXX_DEBUG (logger, string ("found container, end=") +
                           (*partition)->get_end ());
            find_return.connection = get_connection
                ((*partition)->get_host (), (*partition)->get_db ());
            find_return.datatable = (*partition)->get_datatable ();
            return find_return;
        }
        else
        {
            LOG4CXX_ERROR (logger, string ("table ") + tablename + 
                           string (" has a partitioning problem for key ") + 
                           key);
            MyTableException e;
            e.what = "MySQLBackend error";
            throw e;
        }
    }
    else
    {
        MyTableException e;
        e.what = tablename + " not found in directory";
        LOG4CXX_WARN (logger, string ("find_and_checkout: ") + e.what);
        throw e;
    }

    return find_return;
}

FindReturn MySQLBackend::find_next_and_checkout (const string & tablename,
                                                 const string & current_datatable)
{
    Connection * connection = get_connection 
        (this->master_hostname.c_str (), this->master_db.c_str ());

    PreparedStatement * next_statement =
        connection->find_next_statement ("directory");

    StringStringParams * fpp = 
        (StringStringParams*)next_statement->get_bind_params ();
    fpp->set_str1 (tablename.c_str ());
    fpp->set_str2 (current_datatable.c_str ());

    try
    {
        next_statement->execute ();
    }
    catch (MyTableException e)
    {
        destroy_connection (connection);
        throw e;
    }

    FindReturn find_return;
    find_return.connection = NULL;

    if (next_statement->fetch () == MYSQL_NO_DATA)
        return find_return;

    PartitionsResults * fpr =
        (PartitionsResults*)next_statement->get_bind_results ();

    find_return.datatable = fpr->get_datatable ();
    
    find_return.connection = get_connection(fpr->get_host (), fpr->get_db ());

    LOG4CXX_DEBUG (logger, 
                   string ("find_next_and_checkout: hostname=") + 
                   find_return.connection->get_hostname () +
                   string (", db=") + find_return.connection->get_db () +
                   string (", datatable=") + find_return.datatable);

    return find_return;
}

Connection * MySQLBackend::get_connection(const char * hostname, 
                                          const char * db)
{
    map<string, Connection*> * connections = 
        (map<string, Connection*>*) pthread_getspecific(connections_key);

    if (connections == NULL)
    {
        connections = new map<string, Connection*>();
        pthread_setspecific(connections_key, connections);
    }

    string key = string (hostname) + ":" + string (db);
    Connection * connection = (*connections)[key];

    if (!connection)
    {
        connection = new Connection (hostname, db, 
                                     this->master_username.c_str (),
                                     this->master_password.c_str ());
        (*connections)[key] = connection;
    }

    return connection;
}

void MySQLBackend::destroy_connection (Connection * connection)
{
    string key = string (connection->get_hostname ()) + ":" + 
        string (connection->get_db ());
    LOG4CXX_ERROR (logger, string ("destroy_connection: key=") + key);
    map<string, Connection*> * connections = 
        (map<string, Connection*>*) pthread_getspecific(connections_key);
    (*connections)[key] = NULL;
    delete connection;
}

string MySQLBackend::admin (const string & op, const string & data)
{
    if (op == "load_partitions")
    {
        this->load_partitions (data);
        return "done";
    }
    return "";
}
