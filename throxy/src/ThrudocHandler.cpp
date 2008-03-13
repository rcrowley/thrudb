
#ifdef HAVE_CONFIG_H
#include "throxy_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "ThrudocHandler.h"

#if HAVE_LIBUUID
#include <uuid/uuid.h>
#endif

using namespace boost;
using namespace thrudoc;
using namespace log4cxx;
using namespace std;
using namespace facebook::thrift::protocol;

LoggerPtr ThrudocHandler::logger (Logger::getLogger ("ThrudocHandler"));

ThrudocHandler::ThrudocHandler ()
{
    //never invoked (private)
}

ThrudocHandler::ThrudocHandler(shared_ptr<ServiceMonitor> svc_mon)
    : monitor(svc_mon)
{

}

void ThrudocHandler::getBuckets (vector<string> &_return)
{
    vector< shared_ptr<TProtocol> >  protocols = monitor->getConnections( true );

    shared_ptr<MyThrudocClient>  client( new MyThrudocClient() );

    //Send data to each partition
    for(size_t i=0; i<protocols.size(); i++){
        client->setProtocol(protocols[i]);

        client->send_getBuckets();
    }

    map<string, int> all_buckets;

    //Recv data from each partition
    for(size_t i=0; i<protocols.size(); i++){
        client->setProtocol(protocols[i]);

        vector<string> buckets;
        client->recv_getBuckets(buckets);

        //Count buckets from each partition
        for(size_t j=0; j<buckets.size(); j++){
            map<string, int>::iterator it = all_buckets.find( buckets[j] );

            if( it == all_buckets.end() ){
                all_buckets[ buckets[j] ] = 1;
            }else{
                all_buckets[ buckets[j] ]++;
            }
        }
    }

    //
    map<string, int>::iterator it;

    for(it = all_buckets.begin(); it != all_buckets.end(); ++it){

        if(all_buckets.size() != protocols.size()){
            LOG4CXX_ERROR(logger,"Parition missing bucket"+it->first);
        }

        _return.push_back(it->first);
    }

}

void ThrudocHandler::put (const string &bucket, const string &key,
                          const string &value)
{


}

void ThrudocHandler::putValue (string &_return, const string &bucket,
                               const string &value)
{


}

void ThrudocHandler::get (string &_return, const string &bucket,
                          const string &key)
{


}

void ThrudocHandler::remove (const string &bucket, const string &key)
{


}

void ThrudocHandler::scan (ScanResponse &_return, const string &bucket,
                           const string &seed, int32_t count)
{

}

void ThrudocHandler::admin (string &_return, const string &op, const string &data)
{

}

void ThrudocHandler::putList(vector<ThrudocException> &_return,
                             const vector<Element> &elements)
{

}

void ThrudocHandler::getList(vector<ListResponse> &_return,
                             const vector<Element> &elements)
{

}

void ThrudocHandler::removeList(vector<ThrudocException> &_return,
                                const vector<Element> &elements)
{

}

void ThrudocHandler::putValueList(vector<ListResponse> &_return,
                                  const vector<Element> &elements)
{

}

