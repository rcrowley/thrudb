/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

/*
 * TODO:
 * - make error message generic and log the specifics
 * - convert everything to use boost filesystem libs
 */

#include "DiskBackend.h"

#include <fstream>
#include <openssl/md5.h>
#include <stdexcept>
#include <stack>
#include <thrift/concurrency/Mutex.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/progress.hpp>
#include "DistStore.h"
#include "utils.h"

using namespace boost::filesystem;
using namespace diststore;
using namespace facebook::thrift::concurrency;
using namespace log4cxx;
using namespace std;

LoggerPtr DiskBackend::logger (Logger::getLogger ("DiskBackend"));

static Mutex global_mutex = Mutex ();

static int safe_mkdir (const string & path, long mode)
{
    Guard g (global_mutex);

    return mkdir (path.c_str (), mode);
}

static map<string, bool> global_directories = map<string, bool>();

static bool safe_directory_exists (string path)
{
    Guard g (global_mutex);

    if (global_directories.count (path) > 0)
    {
        return true;
    }

    if (directory_exists (path))
    {
        global_directories[path] = true;
        return true;
    }

    return false;
}

DiskBackend::DiskBackend (const string & doc_root)
{
    LOG4CXX_INFO (logger, "DiskBackend: doc_root=" + doc_root);
    this->doc_root = doc_root;
}

vector<string> DiskBackend::getTablenames ()
{
    vector<string> v;
    directory_iterator end_iter;
    for (directory_iterator dir_itr (doc_root); dir_itr != end_iter;
         ++dir_itr)
    {
        if (is_directory (dir_itr->status()))
        {
            v.push_back (dir_itr->path ().leaf ());
        }
    }
    return v;
}

string DiskBackend::get (const string & tablename, const string & key)
{
    string file = build_filename (tablename, key);

    std::ifstream infile;
    infile.open (file.c_str (), ios::in | ios::binary | ios::ate);

    if (!infile.is_open ())
    {
        DistStoreException e;
        e.what = "Error: can't read " + tablename + "/" + key;
        throw e;
    }

    ifstream::pos_type size = infile.tellg ();
    char          *memblock = new char [size];

    infile.seekg (0, ios::beg);
    infile.read (memblock, size);

    infile.close ();

    string obj (memblock, size);

    delete [] memblock;

    return obj;
}

void DiskBackend::put (const string & tablename, const string & key,
                       const string & value)
{
    string dir_p1;
    string dir_p2;
    string dir_p3;
    get_dir_pieces (dir_p1, dir_p2, dir_p3, tablename, key);

    string base = doc_root + "/" + tablename + "/";
    if (!safe_directory_exists (base + dir_p1 + "/" + dir_p2 + "/" + dir_p3))
    {
        if (!safe_directory_exists (base + dir_p1))
        {
            if (0 != safe_mkdir (base + dir_p1, 0777L))
            {
                DistStoreException e;
                e.what = "Could not mkdir: " + base + dir_p1;
                throw e;
            }
        }

        if (!safe_directory_exists (base + dir_p1 + "/" + dir_p2))
        {
            if (0 != safe_mkdir (base + dir_p1 + "/" + dir_p2, 0777L))
            {
                DistStoreException e;
                e.what = "Could not mkdir: " + base + dir_p1 + "/" + dir_p2;
                throw e;
            }
        }

        if (0 != safe_mkdir (base + dir_p1 + "/" + dir_p2 + "/" + dir_p3,
                             0777L))
        {
            DistStoreException e;
            e.what = "Could not mkdir: " + base + dir_p1 + "/" + dir_p2 + "/" +
                dir_p3;
            throw e;
        }
    }

    string file = build_filename (tablename, key);

    std::ofstream outfile;
    outfile.open (file.c_str (), ios::out | ios::binary | ios::trunc);

    if (!outfile.is_open ())
    {
        DistStoreException e;
        e.what = "Error: can't write " + key;
        throw e;
    }

    outfile.write (value.data (), value.size ());

    outfile.close ();
}

void DiskBackend::remove (const string & tablename, const string & key)
{
    string file = build_filename (tablename, key);

    if (file_exists (file))
    {
        if (0 != unlink (file.c_str ()))
        {
            DistStoreException e;
            e.what = "Can't remove " + tablename + "/" + key;
            throw e;
        }
    }
}

string DiskBackend::find_next_file (const string & tablename, 
                                    const string & key)
{
    LOG4CXX_DEBUG (logger, "find_next_file: tablename=" + tablename + 
                   ", key=" + key);

    stack<directory_iterator *> dir_stack;
    directory_iterator * i;
    directory_iterator end;

    string base = doc_root + "/" + tablename + "/";
    if (key.empty ())
    {
        LOG4CXX_DEBUG (logger, "firsts");
        i = new directory_iterator (base);
        dir_stack.push (i);
        if ((*i) != end && is_directory ((*i)->status ()))
        {
            i = new directory_iterator ((*i)->path ());
            dir_stack.push (i);
            if ((*i) != end && is_directory ((*i)->status ()))
            {
                i = new directory_iterator ((*i)->path ());
                dir_stack.push (i);
                if ((*i) != end && is_directory ((*i)->status ()))
                {
                    i = new directory_iterator ((*i)->path ());
                    dir_stack.push (i);
                }
            }
        }
    }
    else
    {
        LOG4CXX_DEBUG (logger, "pieces");
        string d1, d2, d3;
        get_dir_pieces (d1, d2, d3, tablename, key);

        // TODO: fix this if key doesn't exist

        i = new directory_iterator (base);
        dir_stack.push (i);
        // pre-wind to d1
        while ((*i) != end && (*i)->path ().leaf () != d1)
        {
            LOG4CXX_DEBUG (logger, "d1 wind=" + 
                           (*i)->path ().native_file_string ());
            ++(*i);
        }

        i  = new directory_iterator (base + d1);
        dir_stack.push (i);
        // pre-wind to d2
        while ((*i) != end && (*i)->path ().leaf () != d2)
        {
            LOG4CXX_DEBUG (logger, "d2 wind=" + 
                           (*i)->path ().native_file_string ());
            ++(*i);
        }

        i = new directory_iterator (base + d1 + "/" + d2);
        dir_stack.push (i);
        // pre-wind to d3
        while ((*i) != end && (*i)->path ().leaf () != d3)
        {
            LOG4CXX_DEBUG (logger, "d3 wind=" + 
                           (*i)->path ().native_file_string ());
            ++(*i);
        }

        i = new directory_iterator (base + d1 + "/" + d2 + "/" + d3);
        dir_stack.push (i);
        // pre-wind to key
        while ((*i) != end && (*i)->path ().leaf () != key)
        {
            LOG4CXX_DEBUG (logger, "key wind=" + 
                           (*i)->path ().native_file_string ());
            ++(*i);
        }

        LOG4CXX_DEBUG (logger, "winding complete");

        // move past key
        ++(*i);
    }

    // we're in our initial state now

    {
        char buf[128];
        sprintf (buf, "stack dump size=%d", (int)dir_stack.size ());
        LOG4CXX_DEBUG (logger, buf);
    }
    // inital
    i = dir_stack.top ();
    do 
    {

        if ((*i) == end)
        {
            LOG4CXX_DEBUG (logger, "stack.top=(n)");
            // pop
            dir_stack.pop ();
            delete i;
            if (!dir_stack.empty ())
            {
                i = dir_stack.top ();
                // next;
                ++(*i);
            }
        }
        else if (is_regular ((*i)->status ()))
        {
            LOG4CXX_DEBUG (logger, "stack.top(f)=" + 
                           (*i)->path ().native_file_string ());
            string ret = (*i)->path ().leaf ();
            while (!dir_stack.empty ())
            {
                delete dir_stack.top ();
                dir_stack.pop ();
            }
            // ret
            return ret;
        }
        else if (is_directory ((*i)->status ()))
        {
            LOG4CXX_DEBUG (logger, "stack.top(d)=" + 
                           (*i)->path ().native_directory_string ());
            // push
            i = new directory_iterator ((*i)->path ());
            dir_stack.push (i);
        }
        else
        {
            LOG4CXX_DEBUG (logger, "stack.top(?)");
        }
    } while (!dir_stack.empty ());

    LOG4CXX_DEBUG (logger, "ret(Y)=");
    return "";
}

ScanResponse DiskBackend::scan (const string & tablename, const string & seed,
                                int32_t count)
{
    ScanResponse scan_response;

    string ret = find_next_file (tablename, seed);
    while (!ret.empty() && 
           scan_response.elements.size () < (unsigned int)count)
    {
        Element e;
        e.key = ret;
        e.value = get (tablename, ret);
        scan_response.elements.push_back (e);
        ret = find_next_file (tablename, ret);
    } 

    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";
    
    return scan_response;
}

string DiskBackend::admin (const string & op, const string & data)
{
    return "";
}

#define DISK_BACKEND_MAX_TABLENAME_SIZE 33
#define DISK_BACKEND_MAX_KEY_SIZE 33

void DiskBackend::validate (const string * tablename, const string * key,
                            const string * value)
{
    if (tablename && (*tablename).length () >= DISK_BACKEND_MAX_TABLENAME_SIZE)
    {
        DistStoreException e;
        e.what = "tablename too long";
        throw e;
    }
    if (key && (*key).length () >= DISK_BACKEND_MAX_KEY_SIZE)
    {
        DistStoreException e;
        e.what = "key too long";
        throw e;
    }
    /* values can be whatever they want to be */
}

void DiskBackend::get_dir_pieces (string & d1, string & d2, string & d3, 
                                  const string & tablename, const string & key)
{
    // we partition by the md5 of the key so that we'll get an even
    // distrobution of keys across partitions, we still store with key tho
    // TODO: is there a better way to do key -> md5 string
    unsigned char md5[16];
    memset (md5, 0, sizeof (md5));
    MD5 ((const unsigned char *)key.c_str (), key.length (), md5);
    char d1_str[2];
    char d2_str[2];
    char d3_str[2];
    sprintf (d1_str, "%02x", md5[0]);
    sprintf (d2_str, "%02x", md5[1]);
    sprintf (d3_str, "%02x", md5[2]);
    d1 = string (d1_str);
    d2 = string (d2_str);
    d3 = string (d3_str);
}

string DiskBackend::build_filename(const string & tablename, 
                                   const string & key)
{
    string d1, d2, d3;
    get_dir_pieces (d1, d2, d3, tablename, key);
    return 
        doc_root + "/" + 
        tablename + "/" + 
        d1 + "/" +
        d2 + "/" + 
        d3 + "/" + 
        key;
}
