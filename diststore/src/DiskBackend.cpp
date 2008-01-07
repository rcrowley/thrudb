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
 * - convert everything to use boost filesystem libs, is possible/makes sense
 */

#ifdef HAVE_CONFIG_H
#include "diststore_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#if HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO

#include "DiskBackend.h"

#include <fstream>
#include <openssl/md5.h>
#include <stdexcept>
#include <stack>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/progress.hpp>
#include "DistStore.h"
#include "utils.h"

using namespace boost::filesystem;
using namespace diststore;
using namespace log4cxx;
using namespace std;

LoggerPtr DiskBackend::logger (Logger::getLogger ("DiskBackend"));

Mutex DiskBackend::dir_ops_mutex = Mutex ();

int DiskBackend::safe_mkdir (const string & path, long mode)
{
    Guard g (dir_ops_mutex);
    return mkdir (path.c_str (), mode);
}

int DiskBackend::safe_rename (const string & old_path, 
                                     const string & new_path)
{
    Guard g (dir_ops_mutex);
    // NOTE: this is only sorta safe, but probably ok enough
    return rename (old_path.c_str (), new_path.c_str ());
}

bool DiskBackend::safe_directory_exists (string path)
{
    Guard g (dir_ops_mutex);
    return directory_exists (path);
}

DiskBackend::DiskBackend (const string & doc_root)
{
    LOG4CXX_INFO (logger, "DiskBackend: doc_root=" + doc_root);
    this->doc_root = doc_root;
    if (!safe_directory_exists (doc_root))
    {
        safe_mkdir (doc_root, 0777L);
    }
}

vector<string> DiskBackend::getTablenames ()
{
    vector<string> tablenames;
    directory_iterator end_iter;
    for (directory_iterator dir_itr (doc_root); dir_itr != end_iter;
         ++dir_itr)
    {
        if (is_directory (dir_itr->status()) &&
            (dir_itr->path ().leaf ().find ("-del_") == string::npos))
        {
            tablenames.push_back (dir_itr->path ().leaf ());
        }
    }
    return tablenames;
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
                LOG4CXX_ERROR (logger, "put: can't mkdir: " + base + dir_p1);
                DistStoreException e;
                e.what = "DiskBackend error";
                throw e;
            }
        }

        if (!safe_directory_exists (base + dir_p1 + "/" + dir_p2))
        {
            if (0 != safe_mkdir (base + dir_p1 + "/" + dir_p2, 0777L))
            {
                LOG4CXX_ERROR (logger, "put: can't mkdir: " + base + dir_p1 + 
                               "/" + dir_p2);
                DistStoreException e;
                e.what = "DiskBackend error";
                throw e;
            }
        }

        if (0 != safe_mkdir (base + dir_p1 + "/" + dir_p2 + "/" + dir_p3,
                             0777L))
        {
            LOG4CXX_ERROR (logger, "put: can't mkdir: " + base + dir_p1 + "/" +
                           dir_p2 + "/" + dir_p3);
            DistStoreException e;
            e.what = "DiskBackend error";
            throw e;
        }
    }

    string file = build_filename (tablename, key);

    std::ofstream outfile;
    outfile.open (file.c_str (), ios::out | ios::binary | ios::trunc);

    if (!outfile.is_open ())
    {
        DistStoreException e;
        e.what = "Can't write " + tablename + "/" + key;
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

ScanResponse DiskBackend::scan (const string & tablename, const string & seed,
                                int32_t count)
{
    stack<directory_iterator *> dir_stack;
    directory_iterator * i;
    directory_iterator end;

    string base = doc_root + "/" + tablename + "/";
    if (seed.empty ())
    {
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
        string d1, d2, d3;
        get_dir_pieces (d1, d2, d3, tablename, seed);

        // TODO: fix this if seed doesn't exist

        i = new directory_iterator (base);
        dir_stack.push (i);
        // pre-wind to d1
        while ((*i) != end && (*i)->path ().leaf () != d1)
            ++(*i);

        i  = new directory_iterator (base + d1);
        dir_stack.push (i);
        // pre-wind to d2
        while ((*i) != end && (*i)->path ().leaf () != d2)
            ++(*i);

        i = new directory_iterator (base + d1 + "/" + d2);
        dir_stack.push (i);
        // pre-wind to d3
        while ((*i) != end && (*i)->path ().leaf () != d3)
            ++(*i);

        i = new directory_iterator (base + d1 + "/" + d2 + "/" + d3);
        dir_stack.push (i);
        // pre-wind to seed 
        while ((*i) != end && (*i)->path ().leaf () != seed)
            ++(*i);

        // move past seed 
        ++(*i);
    }

    // we're in our initial state now
    ScanResponse scan_response;

    // inital
    i = dir_stack.top ();
    do 
    {

        if ((*i) == end)
        {
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
            string ret = (*i)->path ().leaf ();
            Element e;
            e.key = ret;
            e.value = get (tablename, ret);
            scan_response.elements.push_back (e);
            ++(*i);
        }
        else if (is_directory ((*i)->status ()))
        {
            // push
            i = new directory_iterator ((*i)->path ());
            dir_stack.push (i);
        }
        else
        {
            // unknown, uh-oh
            LOG4CXX_WARN (logger, "stack.top(?)=" + (*i)->path ().leaf ());
        }

        // while we still have dirs on the stack and don't have enough elements
    } while (!dir_stack.empty () && 
             scan_response.elements.size () < (unsigned int)count);

    // clean up after ourselves
    while (!dir_stack.empty ())
    {
        delete dir_stack.top ();
        dir_stack.pop ();
    }

    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";

    return scan_response;
}

string DiskBackend::admin (const string & op, const string & data)
{
    if (op == "create_tablename")
    {
        string base = doc_root + "/" + data;
        if (safe_directory_exists (base))
            return "done";
        if (safe_mkdir (base, 0777L) == 0)
            return "done";
    }
    else if (op == "delete_tablename")
    {
        string base = doc_root + "/" + data;
        if (!safe_directory_exists (base))
            return "done";
        char buf[128];
        sprintf (buf, "%s-del_%06d", base.c_str (), rand ());
        string del_base(buf);
        if (safe_rename (base, del_base) == 0)
            return "done";
    }
    return "";
}

void DiskBackend::validate (const string & tablename, const string * key,
                            const string * value)
{
    DistStoreBackend::validate (tablename, key, value);
    if (tablename.length () > DISK_BACKEND_MAX_TABLENAME_SIZE)
    {
        DistStoreException e;
        e.what = "tablename too long";
        throw e;
    }
    else if (key && (*key) == "")
    {
        DistStoreException e;
        e.what = "invalid key";
        throw e;
    }
    else if (key && (*key).length () > DISK_BACKEND_MAX_KEY_SIZE)
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
    unsigned char md5[16];
    memset (md5, 0, sizeof (md5));
    MD5 ((const unsigned char *)key.c_str (), key.length (), md5);
    char d1_str[2];
    char d2_str[2];
    char d3_str[2];
    sprintf (d1_str, "%02x", md5[0]);
    sprintf (d2_str, "%02x", md5[1]);
    sprintf (d3_str, "%02x", md5[2]);
    d1 = d1_str;
    d2 = d2_str;
    d3 = d3_str;
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

#endif /* HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO */
