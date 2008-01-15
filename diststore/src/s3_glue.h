/*
 * s3_glue.h:
 *
 * Glue logic to make AFFLIB work with Amazon's Simple Storage Service.
 * This can also be compiled stand-alone by #define STAND.
 * That's useful for testing
 *
 * Requires: expat, openssl (for base64 coding)
 */

#ifndef S3_GLUE_H
#define S3_GLUE_H

#if HAVE_LIBEXPAT && HAVE_LIBCURL

#include <stdlib.h>
#include <sys/types.h>

#include <string>
#include <map>
#include <vector>

#include "curl/curl.h"

extern int s3_debug;
extern int s3_retry_max;                // default 5; you can set however you wish
extern int s3_request_retry_count;
extern int s3_object_put_retry_count;
extern const char *aws_access_key_id;
extern const char *aws_secret_access_key;
extern const char *aws_base_url;
extern long long s3_total_written;
extern long long s3_total_read;

#define AMAZON_METADATA_PREFIX "x-amz-meta-"
#define S3_CONTENT_LENGTH "Content-Length"

void s3_audit(int x);

namespace s3 {

    struct s3headers {
        const char *name;                       // do not include x-amz-meta-
        const char *value;
    };

    class buffer {
    public:
        char   *base;                   // array
        size_t  len;                            // length
        int     ptr;                            // for reading
        bool    writable;
        buffer() : base(0),len(0),ptr(0),writable(true) {}
        buffer(const char *base_,int len_) :
            base((char *)base_),len(len_),ptr(0),writable(false) {}
        ~buffer() { if(base && writable) free(base);}
        /* Append bytes; return number of bytes appended */
        size_t write(const char *b,size_t count);
        size_t read(char *b,size_t count);
        void print();
        void clear();
    };

    class response_buffer : public buffer {
    public:
        long   result;                  // HTTP result code
        std::map<std::string,std::string> rheaders;    // response headers
        unsigned char ETag[16];         // if provided, in binary
    };

    /* S3 XML Objects */
    class Contents {
    public:
        std::string Key;
        std::string LastModified;
        std::string ETag;
        size_t Size;
        std::string OwnerID;
        std::string OwnerDisplayName;
        std::string StorageClass;
    };

    class Bucket {
    public:
        std::string Name;
        std::string CreationDate;
    };

    class ListAllMyBucketsResult {
    public:
        ~ListAllMyBucketsResult(){
            for(std::vector<Bucket *>::iterator i = Buckets.begin();
                i != Buckets.end();
                i++){
                delete *i;
            }
        }
        std::string OwnerID;
        std::string OwnerDisplayName;
        std::vector<Bucket *> Buckets;
    };

    class ListBucketResult {
    public:
        ~ListBucketResult(){
            for(std::vector<Contents *>::iterator i = contents.begin();
                i != contents.end();
                i++){
                delete *i;
            }
        }
        std::string Name;
        std::string Prefix;
        std::string Marker;
        int MaxKeys;
        bool IsTruncated;
        std::vector<Contents *> contents;            // list of objects
    };

    class s3_result {
    public:
        s3_result() : depth(0),lambr(0),lbr(0){};
        ~s3_result() {
            if(lambr) delete lambr;
            if(lbr) delete lbr;
        }
        int depth;
        std::string cbuf;                    // buffer of these characters
        class ListAllMyBucketsResult *lambr;
        class ListBucketResult *lbr;                            // list bucket results
        const class buffer *buf;        // what we are parsing
    };

    response_buffer *request(std::string method,std::string path,std::string query,time_t expires,
                             const char *sendbuf,size_t sendbuflen,
                             const s3headers *extra_headers);
    response_buffer *get_url(const char *url);
    s3_result *list_buckets();
    s3_result *list_bucket(std::string bucket,std::string prefix,std::string marker,int max_keys);
    int object_put(std::string bucket,std::string path,
                      const char *buf,size_t buflen,
                      const struct s3headers *meta);
    int bucket_mkdir(std::string bucket);
    int bucket_rmdir(std::string bucket);
    response_buffer *object_get(std::string bucket,std::string path,
                                const s3headers *extra_headers);
    response_buffer *object_head(std::string bucket,std::string path,
                                 const s3headers *extra_headers);
    int object_rm(std::string bucket,std::string path);
}

#endif /* HAVE_LIBEXPAT && HAVE_LIBCURL */

#endif
