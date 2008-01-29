/**
 *
 **/

#ifndef _THRUDOC_PASSTHRU_BACKEND_H_
#define _THRUDOC_PASSTHRU_BACKEND_H_

#include "ThrudocBackend.h"

/**
 * if you override any of these functions you should make sure to do 
 * at least whatever it does...
 */
class ThrudocPassthruBackend : public ThrudocBackend
{
    public:
        ThrudocPassthruBackend () {};
        virtual ~ThrudocPassthruBackend () {};

        virtual std::vector<std::string> getBuckets ()
        {
            return this->get_backend ()->getBuckets ();
        }

        virtual std::string get (const std::string & bucket,
                                 const std::string & key)
        {
            return this->get_backend ()->get (bucket, key);
        }

        virtual void put (const std::string & bucket,
                          const std::string & key,
                          const std::string & value)
        {
            this->get_backend ()->put (bucket, key, value);
        }

        virtual void remove (const std::string & bucket,
                             const std::string & key)
        {
            this->get_backend ()->remove (bucket, key);
        }

        virtual thrudoc::ScanResponse scan (const std::string & bucket,
                                            const std::string & seed,
                                            int32_t count)
        {
            return this->get_backend ()->scan (bucket, seed, count);
        }

        virtual std::string admin (const std::string & op,
                                   const std::string & data)
        {
            return this->get_backend ()->admin (op, data);
        }

        virtual std::vector<thrudoc::ThrudocException> putList
            (const std::vector<thrudoc::Element> & elements)
        {
            return this->get_backend ()->putList (elements);
        }

        virtual std::vector<thrudoc::ListResponse> getList
            (const std::vector<thrudoc::Element> & elements)
        {
            return this->get_backend ()->getList (elements);
        }

        virtual std::vector<thrudoc::ThrudocException> removeList
            (const std::vector<thrudoc::Element> & elements)
        {
            return this->get_backend ()->removeList (elements);
        }

        virtual void validate (const std::string & bucket,
                               const std::string * key,
                               const std::string * value)
        {
            this->get_backend ()->validate (bucket, key, value);
        }

    protected:
        void set_backend (boost::shared_ptr<ThrudocBackend> backend)
        {
            this->backend = backend;
        }

        boost::shared_ptr<ThrudocBackend> get_backend ()
        {
            return this->backend;
        }

    private:
        boost::shared_ptr<ThrudocBackend> backend;
};

#endif
