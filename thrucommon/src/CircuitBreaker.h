#ifndef _CIRCUIT_BREAKER_H_
#define _CIRCUIT_BREAKER_H_ 1

#include <log4cxx/logger.h>
#include <stdint.h>

/* NOTE: this is not thread safe from the standpoint that counts will be exact
 * they will be fuzzy, but generally have the correct behavior and be 
 * preferable to using atomic_count which on some systems uses mutexes. 
 * also atomic_count can't reset to 0 which makes this stuff kinda painful */
class CircuitBreaker
{
    public:
        CircuitBreaker (uint16_t threshold, uint16_t timeout_in_seconds);

        bool allow ();
        void success ();
        void failure ();

    protected:
        void reset ();
        void trip ();

    private:
        static log4cxx::LoggerPtr logger;

        uint16_t threshold;
        uint16_t timeout_in_seconds;

        enum {
            CLOSED,
            OPEN,
            HALF_OPEN
        } state;
        uint32_t failure_count;
        time_t next_check;
};

#endif // _CIRCUIT_BREAKER_H_
