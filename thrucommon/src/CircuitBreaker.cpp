
#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "CircuitBreaker.h"

using namespace log4cxx;

LoggerPtr CircuitBreaker::logger (Logger::getLogger ("CircuitBreaker"));

CircuitBreaker::CircuitBreaker (uint16_t threshold, 
                                uint16_t timeout_in_seconds)
{
    this->threshold = threshold;
    this->timeout_in_seconds = timeout_in_seconds;

    this->reset ();
}

bool CircuitBreaker::allow ()
{
    // if timeout has elapsed give half open a try
    if (this->next_check > time (0))
    {
        this->state = HALF_OPEN;
    }
    return this->state == CLOSED || this->state == HALF_OPEN;
}

void CircuitBreaker::success ()
{
    // if we're half-open and got a success close things up, note that
    // we should never be called if we're in the fully open state, that'd be a 
    // bug in the client code
    if (this->state == HALF_OPEN)
    {
        this->reset ();
    }
}

void CircuitBreaker::failure ()
{
    if (this->state == HALF_OPEN)
    {
        this->trip ();
    }
    else
    {
        ++failure_count;
        // if we've breached our threshold trip
        if (failure_count > this->threshold)
        {
            this->reset ();
        }
    }
}

void CircuitBreaker::reset ()
{
    this->state = CLOSED;
    this->failure_count = 0;
}

void CircuitBreaker::trip ()
{
    if (this->state != OPEN)
    {
        this->state = OPEN;
        this->next_check = time (0) + timeout_in_seconds;
    }
}
