/**
 * Copyright (c) 2008- R.M.
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/

#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "ConfigFile.h"
#include "ReplicationRecorder.h"

#include <iostream>
#include <log4cxx/logger.h>
#include <log4cxx/propertyconfigurator.h>
#include <stdexcept>

using namespace log4cxx;
using namespace std;

LoggerPtr logger (Logger::getLogger ("ThruRecorder"));

//print usage and die
inline void usage ()
{
    cerr<<"thrurecorder -f /path/to/thrurecorder.conf"<<endl;
    exit (-1);
}

int main (int argc, char **argv)
{
    string conf_file = string (getenv ("HOME"))+"/.thrurecorder";

    // Parse args
    for (int i=0; i<argc; i++)
    {
        if (string (argv[i]) == "-f" && (i+1) < argc)
            conf_file = argv[i+1];
    }

    // Read da config
    ConfigManager->readFile ( conf_file );

    try
    {
        //Init logger
        PropertyConfigurator::configure (conf_file);

        LOG4CXX_INFO (logger, "Starting up");

        string replication_private_name =
            ConfigManager->read<string>("REPLICATION_PRIVATE_NAME");
        string replication_name =
            ConfigManager->read<string>("REPLICATION_NAME", "4803@localhost");
        string replication_group =
            ConfigManager->read<string>("REPLICATION_GROUP");

        ReplicationRecorder recorder (replication_name, 
                                      replication_private_name, 
                                      replication_group);
        recorder.record ();

    }
    catch (ReplicationException e)
    {
        LOG4CXX_ERROR (logger, "ReplicationException: " + e.message);
        cerr << "ReplicationException: " << e.message << endl;
    }
    catch (SpreadException e)
    {
        LOG4CXX_ERROR (logger, "SpreadException: " + e.message);
        cerr << "SpreadException: " << e.message << endl;
    }
    catch (std::runtime_error e)
    {
        cerr<<"Runtime Exception: "<<e.what ()<<endl;
    }
    catch (ConfigFile::file_not_found e)
    {
        cerr<<"ConfigFile Fatal Exception: "<<e.filename<<endl;
    }
    catch (ConfigFile::key_not_found e)
    {
        cerr<<"ConfigFile Missing Required Key: "<<e.key<<endl;
    }
    catch (std::exception e)
    {
        cerr<<"Caught Fatal Exception: "<<e.what()<<endl;
    }
    catch (...)
    {
        cerr<<"Caught unknown exception"<<endl;
        }

    return 0;
}

