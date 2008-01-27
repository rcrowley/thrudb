/**
 *
 **/

#ifndef _APP_HELPERS_H_
#define _APP_HELPERS_H_

#include "ThrudocBackend.h"

#include <string>
#include <boost/shared_ptr.hpp>

#ifdef __cplusplus
extern "C" {
#endif

boost::shared_ptr<ThrudocBackend> create_backend (std::string which, 
                                                  int thread_count);

#ifdef __cplusplus
}
#endif

#endif
