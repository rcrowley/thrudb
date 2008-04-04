#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "app_helpers.h"
#include "ConfigFile.h"
#include "utils.h"
#include "CLuceneBackend.h"
#include "LogBackend.h"
#include "ThrudexBackend.h"

#include <boost/shared_ptr.hpp>
#include <log4cxx/logger.h>

using namespace boost;
using namespace std;
using namespace log4cxx;

LoggerPtr logger (Logger::getLogger ("app_helpers"));

shared_ptr<ThrudexBackend> create_backend (string /* which */, 
                                           int /* thread_count */)
{
    string index_root   = ConfigManager->read<string>("INDEX_ROOT");

    setlocale(LC_CTYPE, "en_US.utf8");  //unicode support

    LOG4CXX_INFO(logger, "Starting up");

    shared_ptr<ThrudexBackend> backend(new CLuceneBackend(index_root));

#if HAVE_LIBBOOST_FILESYSTEM
    // NOTE: logging should always be the outtermost backend
    string log_directory =
        ConfigManager->read<string>("LOG_DIRECTORY","");
    if(!log_directory.empty())
    {
        int max_ops = ConfigManager->read<int>("LOG_MAX_OPS", 25000);
        int sync_wait = ConfigManager->read<int>("LOG_SYNC_WAIT", 5000000);
        backend = shared_ptr<ThrudexBackend> (new LogBackend (backend,
                                                              log_directory,
                                                              max_ops,
                                                              sync_wait));
    }
#endif /* HAVE_LIBBOOST_FILESYSTEM */

    return backend;
}
