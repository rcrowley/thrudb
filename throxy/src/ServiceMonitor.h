#ifndef __SERVICE_MONITOR_H__
#define __SERVICE_MONITOR_H__

#include <boost/shared_ptr.hpp>
#include <thrift/transport/TSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

#include <log4cxx/logger.h>

class ServiceNode
{
 public:
    ServiceNode(const std::string name, const std::string host, const int port, const bool master);
    ~ServiceNode();

    bool    ping();
    boost::shared_ptr<facebook::thrift::protocol::TProtocol> getConnection();

    const std::string name;
 protected:
    ServiceNode();

    std::string host;

    int         port;
    bool        master;

    bool        active;
    uint64_t    last_ping;

    boost::shared_ptr<facebook::thrift::transport::TSocket>          socket;
    boost::shared_ptr<facebook::thrift::transport::TFramedTransport> transport;
    boost::shared_ptr<facebook::thrift::protocol::TProtocol>         protocol;

    static log4cxx::LoggerPtr logger;

};


class ServicePartition
{
 public:
    ServicePartition(const std::string name);
    ~ServicePartition();

    std::vector<std::string>       getServiceNodeNames();

    void                           addServiceNode( boost::shared_ptr<ServiceNode> node );
    void                           delServiceNode( std::string name );
    boost::shared_ptr<ServiceNode> getServiceNode( std::string name );

    const std::string name;

 protected:
    ServicePartition();

    std::map< std::string, boost::shared_ptr<ServiceNode> > service_nodes;



    static log4cxx::LoggerPtr logger;

};

class ServiceMonitor
{
 public:
    ServiceMonitor();
    virtual ~ServiceMonitor();

    std::vector<std::string>            getServicePartitionNames();

    void                                addServicePartition( boost::shared_ptr<ServicePartition> part);
    void                                delServicePartition( std::string name );
    boost::shared_ptr<ServicePartition> getServicePartition( std::string name );

 protected:
    std::map< std::string, boost::shared_ptr<ServicePartition> > service_partitions;

    static log4cxx::LoggerPtr logger;

};

#endif
