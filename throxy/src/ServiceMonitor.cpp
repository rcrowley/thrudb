#include "ServiceMonitor.h"

#include <thrift/concurrency/Util.h>

using namespace std;
using namespace log4cxx;
using namespace boost;

using namespace facebook::thrift;
using namespace facebook::thrift::transport;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::concurrency;

LoggerPtr ServiceNode::logger (Logger::getLogger ("ServiceNode"));

ServiceNode::ServiceNode(const string name, const string host, const int port, const bool master)
    : name(name), master(master), host(host), port(port), active(false), last_ping(-1)
{

    socket    = shared_ptr<TSocket>         ( new TSocket(host,port)        );
    transport = shared_ptr<TFramedTransport>( new TFramedTransport(socket)  );
    protocol  = shared_ptr<TProtocol>       ( new TBinaryProtocol(transport));

    this->ping();
}

ServiceNode::ServiceNode()
    : master(false)
{

}

ServiceNode::~ServiceNode()
{

}

bool ServiceNode::ping()
{
    try{

        if( !socket->isOpen() || active == false )
            transport->open();
        active = true;
        return true;

        //Call ping() this doesn't need to exist
        protocol->writeMessageBegin("ping", facebook::thrift::protocol::T_CALL, 0);

        //protocol->writeStructBegin("Thrudoc_ping_args");
        //protocol->writeFieldStop();
        //protocol->writeStructEnd();

        protocol->writeMessageEnd();
        protocol->getTransport()->flush();
        protocol->getTransport()->writeEnd();



        LOG4CXX_INFO(logger,"Connected to remote host");
        int32_t rseqid = 0;
        std::string fname;
        facebook::thrift::protocol::TMessageType mtype = facebook::thrift::protocol::T_CALL;

        //Make sure the call passed back something
        protocol->readMessageBegin(fname, mtype, rseqid);
        protocol->skip(facebook::thrift::protocol::T_STRUCT);
        protocol->readMessageEnd();
        protocol->getTransport()->readEnd();



        if(mtype == facebook::thrift::protocol::T_EXCEPTION ||
           mtype == facebook::thrift::protocol::T_REPLY ){

            //this means the service is working
            last_ping = Util::currentTime();
            active    = true;

        }else{
            throw TException("Service call failed");
        }


    }catch(TTransportException e){

        this->active = false;
        socket->close();

    }catch(TException e){

        this->active = false;
        socket->close();

    }catch(...){

        this->active = false;
        socket->close();
    }

    LOG4CXX_INFO(logger, "ping of "+name+ (active ? "succeded" : "failed"));

    return active;
}

shared_ptr<TProtocol> ServiceNode::getConnection()
{
    if(!active)
        throw TException("Connection closed");


    return protocol;
}


shared_ptr<ServiceNode> ServiceNode::clone()
{
    return shared_ptr<ServiceNode>( new ServiceNode(name,host,port,master) );
}

////////////////////////////////////////////////////////////////////////////

LoggerPtr ServicePartition::logger (Logger::getLogger ("ServicePartition"));

ServicePartition::ServicePartition(const std::string name, unsigned int pool_size)
    : name(name), pool_size(pool_size)
{

}

ServicePartition::~ServicePartition()
{

}

vector<string> ServicePartition::getServiceNodeNames()
{
    vector<string> names;

    map<string, vector<shared_ptr<ServiceNode> > >::iterator it;

    for(it = service_nodes.begin(); it != service_nodes.end(); ++it){
        names.push_back(it->first);
    }

    return names;
}

void ServicePartition::addServiceNode( shared_ptr<ServiceNode> node )
{

    vector< shared_ptr<ServiceNode> > nodes;

    nodes.push_back( node );

    for(unsigned int i=0; i<(pool_size-1); i++){
        nodes.push_back( node->clone() );
    }

    service_nodes[node->name] = nodes;
}

void ServicePartition::delServiceNode( std::string _name )
{
    service_nodes.erase(_name);
}

shared_ptr<ServiceNode> ServicePartition::getServiceNode( std::string _name )
{
    map<string, vector<shared_ptr<ServiceNode> > >::iterator it = service_nodes.find( _name );

    if(it == service_nodes.end())
        throw TException("Unknown node "+_name);

    //return a random connection to this node
    return it->second[ (rand() * 10000 ) % pool_size];
}

shared_ptr<TProtocol> ServicePartition::getConnection( bool master )
{

    map<string, vector< shared_ptr<ServiceNode> > >::iterator it;

    for(it=service_nodes.begin(); it!=service_nodes.end(); ++it){

        if( (master && it->second[0]->master) || !master )
            return it->second[(rand() * 10000 ) % pool_size]->getConnection();

    }

    throw TException("No connections available ");
}



////////////////////////////////////////////////////////////////////////////

LoggerPtr ServiceMonitor::logger (Logger::getLogger ("ServiceMonitor"));

ServiceMonitor::ServiceMonitor()
{

}

ServiceMonitor::~ServiceMonitor()
{

}

vector<string> ServiceMonitor::getServicePartitionNames()
{
    vector<string> names;

    map<string, shared_ptr<ServicePartition> >::iterator it;

    for(it = service_partitions.begin(); it != service_partitions.end(); ++it){
        names.push_back( it->first );
    }

    return names;
}

void ServiceMonitor::addServicePartition( shared_ptr<ServicePartition> part)
{

    service_partitions[part->name] = part;

}

void ServiceMonitor::delServicePartition( string name )
{
    service_partitions.erase(name);
}

shared_ptr<ServicePartition> ServiceMonitor::getServicePartition( string _name )
{
    map<string, shared_ptr<ServicePartition> >::iterator it;

    it = service_partitions.find( _name );

    if( it == service_partitions.end() )
        throw TException("Unknown partition "+_name);

    return it->second;
}



vector< shared_ptr<TProtocol> > ServiceMonitor::getConnections( bool masters )
{

    vector< shared_ptr<TProtocol> > nodes;

    map<string, shared_ptr<ServicePartition> >::iterator it;

    for(it=service_partitions.begin(); it!=service_partitions.end(); ++it){

        nodes.push_back( it->second->getConnection( masters ) );
    }

    return nodes;
}
