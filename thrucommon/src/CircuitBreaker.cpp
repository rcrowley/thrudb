
#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "CircuitBreaker.h"
#include <stdlib.h>

using namespace log4cxx;

LoggerPtr CircuitBreaker::logger (Logger::getLogger ("CircuitBreaker"));

CircuitBreaker::CircuitBreaker (uint16_t threshold, 
                                uint16_t timeout_in_seconds)
{
    char buf[64];
    sprintf (buf, "CircuitBreaker: threshold=%d, timeout_in_seconds=%d",
             threshold, timeout_in_seconds);
    LOG4CXX_INFO (logger, buf);

    this->threshold = threshold;
    this->timeout_in_seconds = timeout_in_seconds;

    this->reset ();
}

bool CircuitBreaker::allow ()
{
    LOG4CXX_DEBUG (logger, "allow: ");
    // if timeout has elapsed give half open a try
    if (this->state == OPEN && this->next_check < time (0))
    {
        LOG4CXX_INFO (logger, "allow:    going half-open");
        this->state = HALF_OPEN;
    }
    return this->state == CLOSED || this->state == HALF_OPEN;
}

void CircuitBreaker::success ()
{
    LOG4CXX_DEBUG (logger, "success: ");
    // if we're half-open and got a success close things up, note that
    // we should never be called if we're in the fully open state, that'd be a 
    // bug in the client code
    if (this->state == HALF_OPEN)
    {
        LOG4CXX_INFO (logger, "success:    in half-open, reset");
        this->reset ();
    }
}

void CircuitBreaker::failure ()
{
    LOG4CXX_DEBUG (logger, "failure: ");
    if (this->state == HALF_OPEN)
    {
        LOG4CXX_INFO (logger, "failure:    in half-open, trip");
        this->trip ();
    }
    else
    {
        ++failure_count;
        // if we've breached our threshold trip
        if (failure_count > this->threshold)
        {
            LOG4CXX_INFO (logger, "failure:    threashold breached,  trip");
            this->trip ();
        }
    }
}

void CircuitBreaker::reset ()
{
    LOG4CXX_INFO (logger, "reset:");
    this->state = CLOSED;
    this->failure_count = 0;
}

void CircuitBreaker::trip ()
{
    LOG4CXX_INFO (logger, "trip:");
    if (this->state != OPEN)
    {
        LOG4CXX_INFO (logger, "trip: tripped");
        this->state = OPEN;
        this->next_check = time (0) + (time_t)timeout_in_seconds;
    }
}
