/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "thrudoc_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#if HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO

#include "DiskBackend.h"

#include <fstream>
#include <openssl/md5.h>
#include <stdexcept>
#include <stack>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include "Thrudoc.h"

namespace fs = boost::filesystem;
using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;

// defines the safety of seed, how many "back-keys" we'll use in case the seed
// key has been deleted. since things are md5'd the likelihood of several in a
// row being deleted between to calls to scan is low, but unfortunatly
// possible.
#define SEED_SEP ';'
#define SEED_SIZE 3

LoggerPtr DiskBackend::logger (Logger::getLogger ("DiskBackend"));

DiskBackend::DiskBackend (const string & doc_root)
{
    LOG4CXX_INFO (logger, "DiskBackend: doc_root=" + doc_root);
    this->doc_root = doc_root;
    if (!fs::is_directory (doc_root))
    {
        try
        {
            fs::create_directories (doc_root);
        }
        catch (exception e)
        {
            LOG4CXX_ERROR (logger, string ("disk error: ") + e.what ());
            throw e;
        }
    }
}

vector<string> DiskBackend::getBuckets ()
{
    vector<string> buckets;
    fs::directory_iterator end_iter;
    for (fs::directory_iterator dir_itr (doc_root); dir_itr != end_iter;
         ++dir_itr)
    {
        if (fs::is_directory (dir_itr->status()) &&
            (dir_itr->path ().leaf ().find ("-del_") == string::npos))
        {
            buckets.push_back (dir_itr->path ().leaf ());
        }
    }
    return buckets;
}

string DiskBackend::get (const string & bucket, const string & key)
{
    string file = build_filename (bucket, key);

    fs::ifstream infile;
    infile.open (file, ios::in | ios::binary | ios::ate);

    if (!infile.is_open ())
    {
        ThrudocException e;
        e.what = "Error: can't read " + bucket + "/" + key;
        throw e;
    }

    fs::ifstream::pos_type size = infile.tellg ();
    char * memblock = new char [size];

    infile.seekg (0, ios::beg);
    infile.read (memblock, size);

    infile.close ();

    string obj (memblock, size);

    delete [] memblock;

    return obj;
}

void DiskBackend::put (const string & bucket, const string & key,
                       const string & value)
{
    string d1;
    string d2;
    string d3;
    get_dir_pieces (d1, d2, d3, bucket, key);

    string loc = doc_root + "/" + bucket + "/" + d1 + "/" + d2 + "/" + d3;
    if (!fs::is_directory (loc))
    {
        fs::create_directories (loc);
    }

    string file = build_filename (bucket, d1, d2, d3, key);

    fs::ofstream outfile;
    outfile.open (file.c_str (), ios::out | ios::binary | ios::trunc);

    if (!outfile.is_open ())
    {
        ThrudocException e;
        e.what = "Can't write " + bucket + "/" + key;
        throw e;
    }

    outfile.write (value.data (), value.size ());

    outfile.close ();
}

void DiskBackend::remove (const string & bucket, const string & key)
{
    string file = build_filename (bucket, key);

    if (fs::is_regular (file))
    {
        if (!fs::remove (file.c_str ()))
        {
            ThrudocException e;
            e.what = "Can't remove " + bucket + "/" + key;
            throw e;
        }
    }
}

ScanResponse DiskBackend::scan (const string & bucket, const string & seed,
                                int32_t count)
{
    stack<fs::directory_iterator *> dir_stack;
    fs::directory_iterator * i;
    fs::directory_iterator end;

    string base = doc_root + "/" + bucket + "/";
    if (seed.empty ())
    {
        i = new fs::directory_iterator (base);
        dir_stack.push (i);
        if ((*i) != end && is_directory ((*i)->status ()))
        {
            i = new fs::directory_iterator ((*i)->path ());
            dir_stack.push (i);
            if ((*i) != end && is_directory ((*i)->status ()))
            {
                i = new fs::directory_iterator ((*i)->path ());
                dir_stack.push (i);
                if ((*i) != end && is_directory ((*i)->status ()))
                {
                    i = new fs::directory_iterator ((*i)->path ());
                    dir_stack.push (i);
                }
            }
        }
    }
    else
    {
        string d1, d2, d3, key;
        {
            // Skip delimiters at beginning.
            string::size_type lastPos = seed.find_first_not_of(SEED_SEP, 0);
            // Find first "non-delimiter".
            string::size_type pos     = seed.find_first_of(SEED_SEP, lastPos);

            key = seed.substr(lastPos, pos - lastPos);
            get_dir_pieces (d1, d2, d3, bucket, key);
            string file = build_filename (bucket, d1, d2, d3, key);
            while ((string::npos != pos || string::npos != lastPos) &&
                   !fs::is_regular (file))
            {
                key = seed.substr(lastPos, pos - lastPos);
                get_dir_pieces (d1, d2, d3, bucket, key);
                file = build_filename (bucket, d1, d2, d3, key);

                // Skip delimiters.  Note the "not_of"
                lastPos = seed.find_first_not_of(SEED_SEP, pos);
                // Find next "non-delimiter"
                pos = seed.find_first_of(SEED_SEP, lastPos);
            }
            LOG4CXX_DEBUG (logger, "scan: using key=" + key);
        }

        i = new fs::directory_iterator (base);
        dir_stack.push (i);
        // pre-wind to d1
        while ((*i) != end && (*i)->path ().leaf () != d1)
            ++(*i);

        i  = new fs::directory_iterator (base + d1);
        dir_stack.push (i);
        // pre-wind to d2
        while ((*i) != end && (*i)->path ().leaf () != d2)
            ++(*i);

        i = new fs::directory_iterator (base + d1 + "/" + d2);
        dir_stack.push (i);
        // pre-wind to d3
        while ((*i) != end && (*i)->path ().leaf () != d3)
            ++(*i);

        i = new fs::directory_iterator (base + d1 + "/" + d2 + "/" + d3);
        dir_stack.push (i);
        // pre-wind to seed 
        while ((*i) != end && (*i)->path ().leaf () != key)
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
            e.value = get (bucket, ret);
            scan_response.elements.push_back (e);
            ++(*i);
        }
        else if (is_directory ((*i)->status ()))
        {
            // push
            i = new fs::directory_iterator ((*i)->path ());
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

    // calculate our new seed
    vector<Element>::reverse_iterator j;
    int size = 0;
    scan_response.seed = "";
    for (j = scan_response.elements.rbegin ();
         size < SEED_SIZE && j != scan_response.elements.rend (); j++, size++)
    {
        scan_response.seed += (*j).key + SEED_SEP;
    }
    LOG4CXX_DEBUG (logger, "scan: scan_response.seed=" + scan_response.seed);

    return scan_response;
}

string DiskBackend::admin (const string & op, const string & data)
{
    if (op == "create_bucket")
    {
        string base = doc_root + "/" + data;
        if (fs::is_directory (base))
            return "done";
        LOG4CXX_INFO (logger, "admin: creating dir=" + data);
        if (fs::create_directories (base))
            return "done";
    }
    else if (op == "delete_bucket")
    {
        try
        {
            string base = doc_root + "/" + data;
            if (!fs::is_directory (base))
                return "done";
            char buf[128];
            sprintf (buf, "%s-del_%06d", base.c_str (), rand ());
            string deleted (buf);
            // we're making a somehwat safe assumption that deleted doesn't 
            // exist
            fs::rename (base, deleted);
        }
        catch (exception & e)
        {
            LOG4CXX_WARN (logger, string ("admin: delete_bucket: what=") +
                          e.what ());
            ThrudocException de;
            de.what = "DiskBackend error";
            throw de;
        }
        return "done";
    }
    return "";
}

void DiskBackend::validate (const string & bucket, const string * key,
                            const string * value)
{
    ThrudocBackend::validate (bucket, key, value);
    if (bucket.length () > DISK_BACKEND_MAX_BUCKET_SIZE)
    {
        ThrudocException e;
        e.what = "bucket too long";
        throw e;
    }
    if (key)
    {
        if ((*key).length () > DISK_BACKEND_MAX_KEY_SIZE)
        {
            ThrudocException e;
            e.what = "key too long";
            throw e;
        }
        else if (key->find (";") != string::npos)
        {
            ThrudocException e;
            e.what = "invalid key";
            throw e;
        }
    }
    /* values can be whatever they want to be */
}

void DiskBackend::get_dir_pieces (string & d1, string & d2, string & d3, 
                                  const string & bucket, const string & key)
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

string DiskBackend::build_filename(const string & bucket, 
                                   const string & key)
{
    string d1, d2, d3;
    get_dir_pieces (d1, d2, d3, bucket, key);
    return build_filename (bucket, d1, d2, d3, key);
}

string DiskBackend::build_filename(const string & bucket, const string & d1,
                                   const string & d2, const string & d3,
                                   const string & key)
{
    return 
        doc_root + "/" + 
        bucket + "/" + 
        d1 + "/" +
        d2 + "/" + 
        d3 + "/" + 
        key;
}

#endif /* HAVE_LIBBOOST_FILESYSTEM && HAVE_LIBCRYPTO */
