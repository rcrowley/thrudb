/**
 *
 **/

#ifdef HAVE_CONFIG_H
#include "diststore_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#if HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM

#include "BDBBackend.h"

#include "DistStore.h"

#include <stdexcept>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;
using namespace diststore;
using namespace log4cxx;
using namespace std;

#define BDB_BACKEND_MAX_TABLENAME_SIZE 32
#define BDB_BACKEND_MAX_KEY_SIZE 64
#define BDB_BACKEND_MAX_VALUE_SIZE 4096

LoggerPtr BDBBackend::logger (Logger::getLogger ("BDBBackend"));

BDBBackend::BDBBackend (const string & bdb_home, const int & thread_count)
{
    LOG4CXX_INFO (logger, "BDBBackend: bdb_home=" + bdb_home);

    this->bdb_home = bdb_home;

    if (!fs::is_directory (bdb_home))
    {
        fs::create_directories (bdb_home);
    }

    try 
    {
        this->db_env = new DbEnv (0);
        u_int32_t env_flags = 
            DB_CREATE     |   // If the environment does not exist, create it.
            DB_RECOVER    |   // run normal recovery
            DB_INIT_LOCK  |   // Initialize locking
            DB_INIT_LOG   |   // Initialize logging
            DB_INIT_MPOOL |   // Initialize the cache
            DB_INIT_TXN   |   // Initialize transactions
            DB_PRIVATE    |   // single process
            DB_THREAD;        // free-threaded (thread-safe)

        // set a max timeout of 1 sec
        this->db_env->set_timeout (1000000, DB_SET_TXN_TIMEOUT);
        // set the maximum number of transactions, 1 per thread
        this->db_env->set_tx_max (thread_count);
        this->db_env->set_lk_detect (DB_LOCK_MINWRITE);
        this->db_env->open (this->bdb_home.c_str (), env_flags, 0);
    } 
    catch (DbException & e)
    {
        LOG4CXX_ERROR (logger, string ("bdb error: ") + e.what ());
        throw e;
    }

}

BDBBackend::~BDBBackend ()
{
    try 
    {
        map<string, Db *>::iterator i;
        for (i = this->dbs.begin (); i != this->dbs.end (); i++)
        {
            (*i).second->close (0);
        }
        this->db_env->close (0);
    } 
    catch (DbException & e) 
    {
        LOG4CXX_ERROR (logger, string ("bdb error: ") + e.what ());
        throw e;
    }
}

vector<string> BDBBackend::getTablenames ()
{
    vector<string> tablenames;
    fs::directory_iterator end_iter;
    for (fs::directory_iterator dir_itr (this->bdb_home); dir_itr != end_iter;
         ++dir_itr)
    {
        if ((fs::is_regular (dir_itr->status ())) && 
            (dir_itr->path ().leaf ().find ("log.") == string::npos))
        {
            tablenames.push_back (dir_itr->path ().leaf ());
        }
        // skipping anything that's not a file or log.*
    }
    return tablenames;
}

string BDBBackend::get (const string & tablename, const string & key)
{
    Dbt db_key;
    db_key.set_data ((char *)key.c_str ());
    db_key.set_size (key.length ());

    Dbt db_value;
    char value[BDB_BACKEND_MAX_VALUE_SIZE + 1];
    db_value.set_data (value);
    db_value.set_ulen (BDB_BACKEND_MAX_VALUE_SIZE + 1);
    db_value.set_flags (DB_DBT_USERMEM);

    try
    {
        if (get_db (tablename)->get (NULL, &db_key, &db_value, 0) != 0)
        {
            DistStoreException e;
            e.what = key + " not found in " + tablename;
            LOG4CXX_DEBUG (logger, string ("get: exception=") + e.what);
            throw e;
        }
    }
    catch (DbDeadlockException & e)
    {
        LOG4CXX_WARN (logger, string ("get: exception=") + e.what ());
        throw e;
    }
    catch (DbException & e)
    {
        LOG4CXX_ERROR (logger, string ("get: exception=") + e.what ());
        throw e;
    }

    return string ((const char *)db_value.get_data (), db_value.get_size ());
}

void BDBBackend::put (const string & tablename, const string & key,
                      const string & value)
{
    Dbt db_key;
    db_key.set_data ((char *)key.c_str ());
    db_key.set_size (key.length ());

    Dbt db_value;
    db_value.set_data ((char *)value.c_str ());
    db_value.set_size (value.length ());

    try
    {
        get_db (tablename)->put (NULL, &db_key, &db_value, 0);
    }
    catch (DbDeadlockException & e)
    {
        LOG4CXX_WARN (logger, string ("put: exception=") + e.what ());
        throw e;
    }
    catch (DbException & e)
    {
        LOG4CXX_ERROR (logger, string ("put: exception=") + e.what ());
        throw e;
    }
}

void BDBBackend::remove (const string & tablename, const string & key)
{
    Dbt db_key;
    db_key.set_data ((char *)key.c_str ());
    db_key.set_size (key.length ());

    try
    {
        get_db (tablename)->del (NULL, &db_key, 0);
    }
    catch (DbDeadlockException & e)
    {
        LOG4CXX_WARN (logger, string ("put: exception=") + e.what ());
        throw e;
    }
    catch (DbException & e)
    {
        LOG4CXX_ERROR (logger, string ("put: exception=") + e.what ());
        throw e;
    }
}

ScanResponse BDBBackend::scan (const string & tablename, const string & seed,
                               int32_t count)
{
    ScanResponse scan_response;

    Dbc * dbc;

    try
    {
        Dbt db_key;
        char key[BDB_BACKEND_MAX_KEY_SIZE + 1];
        db_key.set_data (key);
        db_key.set_ulen (BDB_BACKEND_MAX_KEY_SIZE + 1);
        db_key.set_flags (DB_DBT_USERMEM);
        Dbt db_value;
        char value[BDB_BACKEND_MAX_VALUE_SIZE + 1];
        db_value.set_data (value);
        db_value.set_ulen (BDB_BACKEND_MAX_VALUE_SIZE + 1);
        db_value.set_flags (DB_DBT_USERMEM);

        get_db (tablename)->cursor (NULL, &dbc, 0);

        // this get positions us at the last key we grabbed or the one
        // imediately following it
        // copy over the seed and it's size
        strncpy (key, seed.c_str (), seed.length () + 1);
        db_key.set_size (seed.length () + 1);
        if (dbc->get (&db_key, &db_value, DB_SET_RANGE) == 0)
        {
            strncpy (key, (const char *)db_key.get_data (), 
                     db_key.get_size ());
            key[db_key.get_size ()] = '\0';

            if (seed != key)
            {
                // we got the one after it, it must be gone now, so return 
                // this one
                Element e;
                e.key = string (key, db_key.get_size ());
                e.value = string ((const char *)db_value.get_data (),
                                  db_value.get_size ());
                scan_response.elements.push_back (e);
            } // we'll skip it, it's the one we had last time

            // now keep going until we run out of items or get our fill
            while ((dbc->get (&db_key, &db_value, DB_NEXT) == 0) &&
                   (scan_response.elements.size () < (unsigned int)count))
            {
                strncpy (key, (const char *)db_key.get_data (), 
                         db_key.get_size ());
                key[db_key.get_size ()] = '\0';
                LOG4CXX_ERROR (logger, string ("getn=") + key);

                Element e;
                e.key = string (key, db_key.get_size ());
                e.value = string ((const char *)db_value.get_data (),
                                  db_value.get_size ());
                scan_response.elements.push_back (e);
            }
        }
    }
    catch (DbDeadlockException & e)
    {
        LOG4CXX_WARN (logger, string ("scan: exception=") + e.what ());
        dbc->close ();
        throw e;
    }
    catch (DbException & e)
    {
        LOG4CXX_ERROR (logger, string ("scan: exception=") + e.what ());
        dbc->close ();
        throw e;
    }

    dbc->close ();

    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";

    return scan_response;
}

string BDBBackend::admin (const string & op, const string & data)
{
    if (op == "create_tablename")
    {
        Db * db = NULL;
        try
        {
            db = get_db (data);
        }
        catch (DistStoreException e) {}

        if (!db)
        {
            u_int32_t db_flags = 
                DB_CREATE       |   // allow creating db
                DB_AUTO_COMMIT;     // allow auto-commit   
            db = new Db (this->db_env, 0);
            db->open (NULL,             // Txn pointer
                      data.c_str (),    // file name
                      NULL,             // logical db name
                      DB_BTREE,         // database type
                      db_flags,         // open flags
                      0);               // file mode, defaults
            db->close (0);
        }

        return "done";
    }
    // TODO delete_tablename, but have to figure out how to close the db 
    // handles across all of the threads first...
    return "";
}

void BDBBackend::validate (const string & tablename, const string * key,
                           const string * value)
{
    DistStoreBackend::validate (tablename, key, value);
    if (tablename.length () > BDB_BACKEND_MAX_TABLENAME_SIZE)
    {
        DistStoreException e;
        e.what = "tablename too long";
        throw e;
    }
    else if (key && (*key).length () > BDB_BACKEND_MAX_KEY_SIZE)
    {
        DistStoreException e;
        e.what = "key too long";
        throw e;
    }
    else if (value && (*value).length () > BDB_BACKEND_MAX_VALUE_SIZE)
    {
        DistStoreException e;
        e.what = "value too long";
        throw e;
    }
}

Db * BDBBackend::get_db (const string & tablename)
{
    Db * db = dbs[tablename];
    if (!db)
    {
        u_int32_t db_flags = DB_AUTO_COMMIT; // allow auto-commit   

        db = new Db (this->db_env, 0);
        try
        {
            db->open (NULL,                 // Txn pointer
                      tablename.c_str (),   // file name
                      NULL,                 // logical db name
                      DB_BTREE,             // database type
                      db_flags,             // open flags
                      0);                   // file mode, defaults
            dbs[tablename] = db;
        }
        catch (DbException & e)
        {
            LOG4CXX_WARN (logger, string ("get_open: exception=") + e.what ());
            DistStoreException de;
            de.what = "BDBBackend error";
            throw de;
        }
    }
    return db;
}

#endif /* HAVE_LIBDB_CXX && HAVE_LIBBOOST_FILESYSTEM */
