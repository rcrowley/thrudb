/**
 *
 **/

#ifndef _DISTSTORE_BACKEND_H_
#define _DISTSTORE_BACKEND_H_

#include <string>
#include "DistStore.h"

/**
 *
 **/

class DistStoreBackend
{
    public:
        virtual ~DistStoreBackend () {};

        virtual std::vector<std::string> getTablenames () = 0;
        virtual std::string get (const std::string & tablename,
                                 const std::string & key) = 0;
        virtual void put (const std::string & tablename,
                          const std::string & key,
                          const std::string & value) = 0;
        virtual void remove (const std::string & tablename,
                             const std::string & key) = 0;
        virtual diststore::ScanResponse scan (const std::string & tablename,
                                              const std::string & seed,
                                              int32_t count) = 0;
        virtual std::string admin (const std::string & op,
                                   const std::string & data) = 0;

        virtual std::vector<diststore::DistStoreException> putList
            (const std::vector<diststore::Element> & elements);
        virtual std::vector<diststore::ListResponse> getList
            (const std::vector<diststore::Element> & elements);
        virtual std::vector<diststore::DistStoreException> removeList
            (const std::vector<diststore::Element> & elements);

        // will be called to validate input params through the backend.  should
        // be able to handle NULL's approrpriately. all non,
        // wrapper/passthrough backends should call up to their parents for
        // validate.
        virtual void validate (const std::string & tablename,
                               const std::string * key,
                               const std::string * value);
};


#endif
