/**
 *
 **/

#ifndef __THRUDOC_HANDLER__
#define __THRUDOC_HANDLER__

#include "Thrudoc.h"

#include <string>
#include <log4cxx/logger.h>

#include "ServiceMonitor.h"

class ThrudocHandler : virtual public thrudoc::ThrudocIf {
 public:
    ThrudocHandler (boost::shared_ptr<ServiceMonitor> svc_mon);

    void getBuckets (std::vector<std::string> &_return);

    void put (const std::string &bucket, const std::string &key,
              const std::string &value);

    void putValue (std::string &_return, const std::string &bucket,
                   const std::string &value);

    void get (std::string &_return, const std::string &bucket,
              const std::string &key);

    void remove (const std::string &bucket, const std::string &key);

    void scan (thrudoc::ScanResponse &_return,
               const std::string &bucket,
               const std::string &seed, int32_t count);

    void putList(std::vector<thrudoc::ThrudocException> &_return,
                 const std::vector<thrudoc::Element> &elements);

    void getList(std::vector<thrudoc::ListResponse> &_return,
                 const std::vector<thrudoc::Element> &elements);

    void removeList(std::vector<thrudoc::ThrudocException> &_return,
                    const std::vector<thrudoc::Element> &elements);

    void putValueList(std::vector<thrudoc::ListResponse> &_return,
                      const std::vector<thrudoc::Element> &elements);

    void admin (std::string &_return, const std::string &op,
                const std::string &data);

 private:
    ThrudocHandler();

    static log4cxx::LoggerPtr         logger;

    boost::shared_ptr<ServiceMonitor> monitor;


};


class MyThrudocClient : public thrudoc::ThrudocClient
{
 public:
 MyThrudocClient():
    ThrudocClient(boost::shared_ptr<facebook::thrift::protocol::TProtocol>()){};

    //Let me reuse this client for each connection
    void setProtocol(boost::shared_ptr<facebook::thrift::protocol::TProtocol> p){
        this->piprot_ = p;
        poprot_ = p;
        iprot_ = p.get();
        oprot_ = p.get();
    }

};

#endif
