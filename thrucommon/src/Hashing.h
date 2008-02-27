/**
 *
 **/

#ifndef _HASHING_CONNECTION_H_
#define _HASHING_CONNECTION_H_

#include <stdexcept>

class HashingException : public std::exception
{
    public:
        HashingException () {}

        HashingException (const std::string & message) :
            message (message) {}

        ~HashingException () throw () {}

        std::string message;
};

class Hashing
{
    public:
        Hashing () {};
        virtual ~Hashing () {};

        virtual double get_point (std::string key) = 0;
};

class FNV32Hashing : public Hashing
{
    public:
        FNV32Hashing () {};

        double get_point (std::string key);

    private:
        static uint32_t FNV_32_INIT;
        static uint32_t FNV_32_PRIME;
};

#endif /* _HASHING_CONNECTION_H_ */
