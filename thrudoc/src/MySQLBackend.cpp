#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#if HAVE_LIBMYSQLCLIENT_R

#include "MySQLBackend.h"

/*
 * TODO:
 * - timeout the directory info at some interval
 * - think about straight key parititioning to allow in order scans etc...
 * - look at libmemcached for it's partitioning algoritms
 */

using namespace thrudoc;
using namespace log4cxx;
using namespace mysql;
using namespace std;

// private
LoggerPtr MySQLBackend::logger (Logger::getLogger ("MySQLBackend"));

class Partition
{
    public:
        static bool greater (Partition * a, Partition * b)
        {
            return a->get_end () < b->get_end ();
        }

        Partition (const double & end)
        {
            this->end = end;
            this->hostname = NULL;
            this->slave_hostname = NULL;
            this->db = NULL;
            this->datatable = NULL;
        }

        Partition (mysql::PartitionResults * partition_results)
        {
            this->end = partition_results->get_end ();
            this->hostname = strdup (partition_results->get_hostname ());
            this->port = partition_results->get_port ();
            const char * tmp = partition_results->get_slave_hostname ();
            if (tmp)
                this->slave_hostname = strdup (tmp);
            else
                this->slave_hostname = NULL;
            this->slave_port = partition_results->get_slave_port ();
            this->db = strdup (partition_results->get_db ());
            this->datatable = strdup (partition_results->get_datatable ());
        }

        ~Partition ()
        {
            if (hostname)
                free (hostname);
            if (slave_hostname)
                free (slave_hostname);
            if (db)
                free (db);
            if (datatable)
                free (datatable);
        }

        double get_end ()
        {
            return this->end;
        }

        const char * get_hostname ()
        {
            return this->hostname;
        }

        const int get_port ()
        {
            return this->port;
        }

        const char * get_slave_hostname ()
        {
            return this->slave_hostname;
        }

        const int get_slave_port ()
        {
            return this->slave_port;
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
        double end;
        char * hostname;
        short port;
        char * slave_hostname;
        short slave_port;
        char * db;
        char * datatable;
};


MySQLBackend::MySQLBackend (const string & master_hostname, 
                            const short master_port,
                            const string & slave_hostname, 
                            const short slave_port,
                            const string & directory_db,
                            const string & username, const string & password, 
                            int max_value_size)
{
    {
        char buf[256];
        sprintf (buf, "MySQLBackend: master_hostname=%s, master_port=%d, slave_hostname=%s, slave_port=%d, directory_db=%s, username=%s, password=****, max_value_size=%d\n", 
                 master_hostname.c_str (), master_port, slave_hostname.c_str (), 
                 slave_port, directory_db.c_str (), username.c_str (), 
                 max_value_size);
        LOG4CXX_INFO (logger, buf);
    }
    this->master_hostname = master_hostname;
    this->master_port = master_port;
    this->slave_hostname = slave_hostname;
    this->slave_port = slave_port;
    this->directory_db = directory_db;
    this->username = username;
    this->password = password;
    this->max_value_size = max_value_size;

    this->connection_factory = new ConnectionFactory ();
}

MySQLBackend::~MySQLBackend ()
{
    map<string, set<Partition*, bool(*)(Partition*, Partition*)>* >::iterator i;
    for (i = partitions.begin (); i != partitions.end (); i++)
    {
        set<Partition*, bool(*)(Partition*, Partition*)>::iterator j;
        for (j = (*i).second->begin (); j != (*i).second->end (); j++)
            delete (*j);  // each partition
        delete (*i).second; // the set
    }
    delete this->connection_factory;
}

set<Partition*, bool(*)(Partition*, Partition*)> * 
MySQLBackend::load_partitions (const string & bucket)
{
    LOG4CXX_INFO (logger, string ("load_partitions: bucket=") + bucket);

    Connection * connection = connection_factory->get_connection
        (this->master_hostname.c_str (), this->master_port, 
         this->slave_hostname.c_str (), this->slave_port, 
         this->directory_db.c_str (), this->username.c_str (),
         this->password.c_str ());

    PreparedStatement * partitions_statement =
        connection->find_partitions_statement ();

    StringParams * fp = (StringParams*)partitions_statement->get_bind_params ();
    fp->set_str (bucket.c_str ());

    partitions_statement->execute ();

    PartitionResults * pr =
        (PartitionResults*)partitions_statement->get_bind_results ();

    set<Partition*, bool(*)(Partition*, Partition*)> *
        new_partitions = new set<Partition*, bool(*)(Partition*, Partition*)>
        (Partition::greater);

    while (partitions_statement->fetch () != MYSQL_NO_DATA)
    {
        LOG4CXX_INFO (logger, string ("  load_partitions inserting: datatable=") +
                      pr->get_datatable ());
        new_partitions->insert (new Partition (pr));
    }

    partitions_statement->free_result ();

    if (new_partitions->size () > 0)
    {
        set<Partition*, bool(*)(Partition*, Partition*)> * old_partitions = 
            partitions[bucket];
        if (old_partitions)
        {
            set<Partition*, bool(*)(Partition*, Partition*)>::iterator i;
            for (i = old_partitions->begin (); 
                 i != old_partitions->end (); i++)
                delete (*i);  // each partition
            delete old_partitions; // the set
        }
        partitions[bucket] = new_partitions;
    }
    else
    {
        LOG4CXX_WARN (logger, string ("load_partitions: request to load ") + 
                      bucket + string (" with no partitions"));
        delete new_partitions;
        new_partitions = NULL;
    }

    return new_partitions;
}

vector<string> MySQLBackend::getBuckets ()
{
    vector<string> buckets;
    map<string, set<Partition*, bool(*)(Partition*, Partition*)>* >::iterator i;
    for (i = partitions.begin (); i != partitions.end (); i++)
    {
        buckets.push_back ((*i).first);
    }
    return buckets;
}

string MySQLBackend::get (const string & bucket, const string & key )
{
    FindReturn find_return = this->find_and_checkout (bucket, key);

    PreparedStatement * get_statement =
        find_return.connection->find_get_statement
        (find_return.datatable.c_str (), this->max_value_size);

    StringParams * tkp = (StringParams*)get_statement->get_bind_params ();
    tkp->set_str (key.c_str ());

    get_statement->execute ();

    string value;
    if (get_statement->fetch () == MYSQL_NO_DATA)
    {
        ThrudocException e;
        e.what = key + " not found in " + bucket;
        LOG4CXX_DEBUG (logger, string ("get: ") + e.what);
        throw e;
    }

    KeyValueResults * kvr =
        (KeyValueResults*)get_statement->get_bind_results ();
    LOG4CXX_DEBUG (logger, string ("get: key=") + kvr->get_key () +
                   string (", value=") + kvr->get_value ());
    value = kvr->get_value ();

    get_statement->free_result ();

    return value;
}

void MySQLBackend::put (const string & bucket, const string & key, const string & value)
{
    FindReturn find_return = this->find_and_checkout (bucket, key);

    PreparedStatement * put_statement =
        find_return.connection->find_put_statement
        (find_return.datatable.c_str ());

    StringStringParams * kvp = (StringStringParams*)put_statement->get_bind_params ();
    kvp->set_str1 (key.c_str ());
    kvp->set_str2 (value.c_str ());

    put_statement->execute ();
}

void MySQLBackend::remove (const string & bucket, const string & key )
{
    FindReturn find_return = this->find_and_checkout (bucket, key);

    PreparedStatement * delete_statement =
        find_return.connection->find_delete_statement
        (find_return.datatable.c_str ());

    StringParams * kvp = (StringParams*)delete_statement->get_bind_params ();
    kvp->set_str (key.c_str ());

    delete_statement->execute ();
}

void MySQLBackend::append(
	const string & bucket,
	const string & key,
	const string & value
) {
	FindReturn find_return = this->find_and_checkout(bucket, key);
	PreparedStatement * append_statement =
		find_return.connection->find_append_statement(
		find_return.datatable.c_str());
	StringStringParams * kvp =
		(StringStringParams *)append_statement->get_bind_params();
	kvp->set_str1(key.c_str());
	kvp->set_str2(value.c_str());
	append_statement->execute();
}

string MySQLBackend::scan_helper (ScanResponse & scan_response,
                                  FindReturn & find_return,
                                  const string & seed,
                                  int32_t count)
{
    PreparedStatement * scan_statement =
        find_return.connection->find_scan_statement
        (find_return.datatable.c_str (), this->max_value_size);

    StringIntParams * kcp =
        (StringIntParams*)scan_statement->get_bind_params ();
    kcp->set_str (seed.c_str ());
    kcp->set_i (count);

    scan_statement->execute ();

    int ret;
    KeyValueResults * kvr =
        (KeyValueResults*)scan_statement->get_bind_results ();
    while ((ret = scan_statement->fetch ()) == 0)
    {
        // we gots results
        Element e;
        e.key = kvr->get_key ();
        e.value = kvr->get_value ();
        scan_response.elements.push_back (e);
    }

    scan_statement->free_result ();

    return scan_response.elements.size() > 0 ? 
        scan_response.elements.back ().key : "";
}

/*
 * our seed will just be the last key returned. that's enough for us to find
 * the partition used last
 */
ScanResponse MySQLBackend::scan (const string & bucket, const string & seed,
                                 int32_t count)
{
    // base datatable should be > "0"
    FindReturn find_return;
    if (seed != "")
    {
        // subsequent call, use the normal find method
        find_return = this->find_and_checkout (bucket, seed);
    }
    else
    {
        // first call, get the first datatable
        find_return = this->find_next_and_checkout (bucket, "0");
    }
    LOG4CXX_DEBUG (logger, 
                   string ("scan: hostname=") + 
                   find_return.connection->get_hostname () +
                   string (", db=") + find_return.connection->get_db () +
                   string (", datatable=") + find_return.datatable);

    ScanResponse scan_response;

    int size = 0;
    string offset = seed;

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
        find_return = this->find_next_and_checkout (bucket,
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

FindReturn MySQLBackend::find_and_checkout (const string & bucket,
                                            const string & key)
{
    double point = hashing.get_point (key);

    if (logger->isDebugEnabled())
    {
        char buf[128];
        sprintf (buf, "key=%s -> point=%f", key.c_str (), point);
        LOG4CXX_DEBUG (logger, buf);
    }

    FindReturn find_return;

    // look for the partitions set, this god awful mess is b/c std::map []
    // creates elements if they don't exist and find returns the last element
    // if what you're looking for doesn't exists, who the fuck came up with
    // this shit.
    set<Partition*, bool(*)(Partition*, Partition*)> * partitions_set = NULL;
    if (partitions.find (bucket) != partitions.end ())
    {
        partitions_set = partitions[bucket];
    }

    if (partitions_set == NULL)
    {
        // we didn't find it, try loading
        partitions_set = this->load_partitions (bucket);
    }

    if (partitions_set != NULL)
    {
        // we now have the partitions set
        Partition * part = new Partition (point);
        // look for the matching partition
        set<Partition*>::iterator partition = partitions_set->lower_bound (part);
        delete part;
        if (partition != partitions_set->end ())
        {
            LOG4CXX_DEBUG (logger, string ("found container, datatable=") +
                           (*partition)->get_datatable ());
            find_return.connection = connection_factory->get_connection
                ((*partition)->get_hostname (), (*partition)->get_port (),
                 (*partition)->get_slave_hostname (), 
                 (*partition)->get_slave_port (), (*partition)->get_db (),
                 this->username.c_str (), this->password.c_str ());
            find_return.datatable = (*partition)->get_datatable ();
            return find_return;
        }
        else
        {
            LOG4CXX_ERROR (logger, string ("table ") + bucket + 
                           string (" has a partitioning problem for key ") + 
                           key);
            ThrudocException e;
            e.what = "MySQLBackend error";
            throw e;
        }
    }
    else
    {
        ThrudocException e;
        e.what = bucket + " not found in directory";
        LOG4CXX_WARN (logger, string ("find_and_checkout: ") + e.what);
        throw e;
    }

    return find_return;
}

FindReturn MySQLBackend::find_next_and_checkout (const string & bucket,
                                                 const string & current_datatable)
{
    Connection * connection = connection_factory->get_connection 
        (this->master_hostname.c_str (), this->master_port,
         this->slave_hostname.c_str (), this->slave_port,
         this->directory_db.c_str (), this->username.c_str (), 
         this->password.c_str ());

    PreparedStatement * next_statement =
        connection->find_next_statement ();

    StringStringParams * fpp = 
        (StringStringParams*)next_statement->get_bind_params ();
    fpp->set_str1 (bucket.c_str ());
    fpp->set_str2 (current_datatable.c_str ());

    next_statement->execute ();

    FindReturn find_return;
    find_return.connection = NULL;

    if (next_statement->fetch () == MYSQL_NO_DATA)
        return find_return;

    PartitionResults * fpr =
        (PartitionResults*)next_statement->get_bind_results ();

    find_return.datatable = fpr->get_datatable ();

    find_return.connection = connection_factory->get_connection
        (fpr->get_hostname (), fpr->get_port (), fpr->get_slave_hostname (),
         fpr->get_slave_port (), fpr->get_db (), this->username.c_str (),
         this->password.c_str ());

    next_statement->free_result ();

    LOG4CXX_DEBUG (logger, 
                   string ("find_next_and_checkout: hostname=") + 
                   find_return.connection->get_hostname () +
                   string (", db=") + find_return.connection->get_db () +
                   string (", datatable=") + find_return.datatable);

    return find_return;
}

string MySQLBackend::admin (const string & op, const string & data)
{
    string ret = ThrudocBackend::admin (op, data);
    if (!ret.empty ())
    {
        return ret;
    }
    else if (op == "load_partitions")
    {
        this->load_partitions (data);
        return "done";
    }
    return "";
}

void MySQLBackend::validate (const string & bucket, const string * key, 
                             const string * value)
{
    ThrudocBackend::validate (bucket, key, value);
    if (bucket.length () > MYSQL_BACKEND_MAX_BUCKET_SIZE)
    {
        ThrudocException e;
        e.what = "bucket too long";
        throw e;
    }
    else if (key && (*key).length () > MYSQL_BACKEND_MAX_KEY_SIZE)
    {
        ThrudocException e;
        e.what = "key too long";
        throw e;
    }
    else if (value && (*value).length () > (unsigned int)this->max_value_size)
    {
        ThrudocException e;
        e.what = "value too long";
        throw e;
    }
}

#endif /* HAVE_LIBMYSQLCLIENT_R */
