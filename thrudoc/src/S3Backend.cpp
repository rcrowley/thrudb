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

#if HAVE_LIBEXPAT && HAVE_LIBCURL

#include "S3Backend.h"

#include "Thrudoc.h"
#include "s3_glue.h"

#include <fstream>
#include <stdexcept>

using namespace s3;
using namespace std;
using namespace thrudoc;
using namespace log4cxx;

LoggerPtr S3Backend::logger (Logger::getLogger ("S3Backend"));

S3Backend::S3Backend (string bucket_prefix)
{
    LOG4CXX_INFO (logger, "S3Backend: bucket_prefix=" + bucket_prefix);
    this->bucket_prefix = bucket_prefix;
}

vector<string> S3Backend::getBuckets ()
{
    vector<string> buckets;

    s3_result * result = list_buckets ();

    if (!result)
    {
        ThrudocException e;
        e.what = "S3Backend error";
        throw e;
    }

    vector<Bucket *> contents = result->lambr->Buckets;
    vector<Bucket *>::iterator i;
    for (i = contents.begin (); i != contents.end (); i++)
    {
        if (this->bucket_prefix.empty ())
        {
            // everything is a legal bucketname
            buckets.push_back ((*i)->Name);
        }
        else if ((*i)->Name.find (this->bucket_prefix) != string::npos)
        {
            // we're using a prefix, so only things that begin with it are
            // legal bucket names
            buckets.push_back ((*i)->Name.substr 
                               (this->bucket_prefix.length ()));
        }
    }

    delete result;

    return buckets;
}

string S3Backend::get (const string & bucket, const string & key)
{
    class response_buffer *b = NULL;

    b = object_get (this->bucket_prefix + bucket, key, 0);

    if(b == NULL){
        ThrudocException e;
        e.what = "S3Backend error";
        throw e;
    }

    if(b->result != 200) {
        delete b;
        ThrudocException e;
        e.what = "S3: " + key + " not found";
        throw e;
    }

    string result(b->base,b->len);
    delete b;

    return result;
}

void S3Backend::put (const string & bucket, const string & key,
                     const string & value)
{
    struct s3headers meta[2] = {{0,0},{0,0}};

    int r = object_put (this->bucket_prefix + bucket, key, value.c_str(),
                        value.length(), meta);

    if (r == -1)
    {
        ThrudocException e;
        e.what = "S3Backend error";
        throw e;
    }
}

void S3Backend::remove (const string & bucket, const string & key)
{
    int r = object_rm (this->bucket_prefix + bucket, key);

    if (r == -1)
    {
        ThrudocException e;
        e.what = "S3Backend error";
        throw e;
    }
}

void S3Backend::append(
	const string & bucket,
	const string & key,
	const string & value
) {
	ThrudocException e;
	e.what = "append only available to MySQL backend";
	throw e;
}

ScanResponse S3Backend::scan (const string & bucket, const string & seed,
                              int32_t count)
{
    ScanResponse scan_response;

    s3_result * result = list_bucket (this->bucket_prefix + bucket, "", seed,
                                      count);

    if (!result)
    {
        ThrudocException e;
        e.what = "S3Backend error";
        throw e;
    }

    vector<Contents *> contents = result->lbr->contents;
    vector<Contents *>::iterator i;
    for (i = contents.begin (); i != contents.end (); i++)
    {
        Element e;
        e.key = (*i)->Key;
        // this isn't going to use the full get stack, that might be a problem
        // in some set ups (that aren't currently possible,) but it's also a
        // benefit in that it won't fill up the cache with stuff that's only
        // going to be fetched a single time for the scan. NOTE: if this isn't
        // the base persistent backend then this scan shouldn't be used.
        e.value = get (bucket, e.key);
        scan_response.elements.push_back (e);
    }

    scan_response.seed = scan_response.elements.size () > 0 ?
        scan_response.elements.back ().key : "";

    delete result;

    return scan_response;
}

string S3Backend::admin (const string & op, const string & data)
{
    string ret = ThrudocBackend::admin (op, data);
    if (!ret.empty ())
    {
        return ret;
    }
    else if (op == "create_bucket")
    {
        // will throw
        validate (data, NULL, NULL);

        ThrudocException e;
        class response_buffer * b =  request ("PUT", this->bucket_prefix + data,
                                              "", 0, 0, 0, 0);
        if( b == NULL )
            e.what = "S3Backend error";
        int result = b->result;
        delete b;
        switch(result)
        {
            case 200:
                return "done";
            case 409:
                e.what = "bucket " + data + " already exists";
        }
        LOG4CXX_WARN (logger, e.what);
        throw e;
    }
    else if (op == "delete_bucket")
    {
        // will throw
        validate (data, NULL, NULL);

        // NOTE: only works if empty...
        class response_buffer * b = request ("DELETE", 
                                             this->bucket_prefix + data, "", 0,
                                             0, 0, 0);
        int result = b->result;
        delete b;
        ThrudocException e;
        switch(result)
        {
            case 204:
            case 200:
                return "done";
            case 403:
                e.what = "S3Backend error:EACCES";
                break;
            case 404:
                e.what = "S3Backend error: ENOENT";
                break;
            case 409:
                e.what = "S3Backend error: ENOTEMPTY";
                break;
        }
        LOG4CXX_WARN (logger, e.what);
        throw e;
    }
    return "";
}

void S3Backend::validate (const string & bucket, const string * key,
                          const string * value)
{
    ThrudocBackend::validate (bucket, key, value);

    // NOTE: s3 is going to validate them for us so do we really need to?

    // buckets
    // - lowercase letters, numbers, periods, underscores, and dashes
    // - start with a number or letter
    // - must be between 3 and 255 chars
    // - can not look like an ip address


    // key/value, no stated restrictions, value has a max size well beyond what
    // you can get in our api so...

}

#endif /* HAVE_LIBEXPAT && HAVE_LIBCURL */
