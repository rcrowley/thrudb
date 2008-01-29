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

#include "BloomBackend.h"
#include "bloom_filter.hpp"

using namespace facebook::thrift::concurrency;
using namespace thrudoc;
using namespace boost;
using namespace std;
using namespace log4cxx;

LoggerPtr BloomBackend::logger (Logger::getLogger ("BloomBackend"));

BloomBackend::BloomBackend( shared_ptr<ThrudocBackend> backend )
    : filter_space(10000000)
{
    LOG4CXX_INFO(logger,"BloomBackend");
    this->set_backend (backend);

    hit_filter   = new bloom_filter(filter_space,1.0/(1.0 * filter_space), ((int) 100000*rand()));
    miss_filter  = new bloom_filter(filter_space,1.0/(1.0 * filter_space), ((int) 100000*rand()));
    miss_filter2 = new bloom_filter(filter_space,1.0/(1.0 * filter_space), ((int) 100000*rand()));

    LOG4CXX_INFO(logger,"Started");
}


BloomBackend::~BloomBackend()
{
    delete hit_filter;
    delete miss_filter;
    delete miss_filter2;
}

string BloomBackend::get (const string & bucket, const string & key)
{
    string bloom_key = bucket + key;

    string value;
    bool   checked_backend = false;

    //hit on bloom
    if(hit_filter->contains(bloom_key)){
        checked_backend = true;

        try{
            value = this->get_backend ()->get(bucket,key);
        }catch(ThrudocException e){

        }

        //Got it, done...
        if(!value.empty())
            return value;

    }


    //is this a definate miss? leave now
    if(miss_filter->contains(bloom_key) && miss_filter2->contains(bloom_key))
        return value;


    //the backend was a miss, add it to miss filter
    if(checked_backend){
        Guard g(mutex);

        miss_filter->insert(bloom_key);
        miss_filter2->insert(bloom_key);

    }else{
        //this must be a new id check backend

        try{
            value = this->get_backend ()->get(bucket,key);
        }catch(ThrudocException e){

        }

        if(value.empty()){
            Guard g(mutex);
            miss_filter->insert(bloom_key);
            miss_filter2->insert(bloom_key);
        }else{
            Guard g(mutex);
            hit_filter->insert(bloom_key);
        }
    }


    return value;

}

void BloomBackend::put (const string & bucket, const string & key, const string & value)
{
    this->get_backend ()->put(bucket,key,value);

    string bloom_key = bucket+key;

    if(!hit_filter->contains(bloom_key)){
        Guard g(mutex);

        hit_filter->insert(bloom_key);
    }
}
