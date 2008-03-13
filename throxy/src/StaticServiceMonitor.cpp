#include "StaticServiceMonitor.h"
#include "utils.h"

using namespace std;
using namespace log4cxx;
using namespace boost;

using namespace facebook::thrift;

LoggerPtr StaticServiceMonitor::logger(Logger::getLogger("StaticServiceMonitor"));

#define SVC_LIMIT 1000

StaticServiceMonitor::StaticServiceMonitor()
{
    //private
}

StaticServiceMonitor::StaticServiceMonitor(const string conf_file)
    : config_file(conf_file)
{
    if(!file_exists(conf_file)){
        throw TException("Config file missing: "+conf_file);
    }

    config.readFile(conf_file);

    char p_name[128];
    char n_name[128];

    //Create partitions
    for(int i=1; i<SVC_LIMIT; i++) {
        sprintf(p_name,"PARTITION_%d",i);

        if( !config.keyExists(p_name) )
            break;

        shared_ptr<ServicePartition> part(new ServicePartition(p_name));

        LOG4CXX_INFO(logger, string("Partition added: ")+p_name);

        //Add service nodes for this partition
        for(int j=1; j<SVC_LIMIT; j++){

            sprintf(n_name,"%s_NODE_%d",p_name,j);

            if( !config.keyExists(string(n_name)+"_NAME") )
                break;

            LOG4CXX_INFO(logger, string("Partition node added: ")+n_name);

            string name   = config.read<string>(string(n_name)+"_NAME");
            string host   = config.read<string>(string(n_name)+"_HOST");
            int    port   = config.read<int>   (string(n_name)+"_PORT",-1);
            bool   master = config.read<bool>  (string(n_name)+"_MASTER",false);

            shared_ptr<ServiceNode> node( new ServiceNode(name,host,port,master) );

            part->addServiceNode(node);
        }

        this->addServicePartition(part);
    }

    cerr<<"Done"<<endl;
}


StaticServiceMonitor::~StaticServiceMonitor()
{

}


