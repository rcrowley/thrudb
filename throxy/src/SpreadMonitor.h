#ifndef __SPREAD_MONITOR_H__
#define __SPREAD_MONITOR_H__

#include <boost/shared_ptr.hpp>
#include <concurrency/Thread.h>
#include <concurrency/Mutex.h>
#include <concurrency/Monitor.h>
#include <concurrency/PosixThreadFactory.h>

#include <sys/types.h>
#include <sp.h>
#include <event.h>

#include <log4cxx/logger.h>

#include "utils.h"

#include <vector>
#include <map>
#include <string>


class SpreadMonitor : public facebook::thrift::concurrency::Runnable
{
 public:
    SpreadMonitor();
    ~SpreadMonitor();

    void run();

 private:

};

#endif
