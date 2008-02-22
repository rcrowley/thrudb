#ifndef __THRUDEX_PASSTHRU_BACKEND_H__
#define __THRUDEX_PASSTHRU_BACKEND_H__

#include "ThrudexBackend.h"

class ThrudexPassthruBackend : public ThrudexBackend
{
 public:
    ThrudexPassthruBackend() {};
    virtual ~ThrudexPassthruBackend() {};

    virtual std::vector<std::string> getIndices() 
    {
      return this->get_backend ()->getIndices ();
    }

    virtual void put(const thrudex::Document &d)
    {
      this->get_backend ()->put (d);
    }

    virtual void remove(const thrudex::Element &e)
    {
      this->get_backend ()->remove (e);
    }

    // TODO: why's the response a param instead of a return?
    virtual void search(const thrudex::SearchQuery &s, thrudex::SearchResponse &r)
    {
      this->get_backend ()->search (s, r);
    }

    virtual std::vector<thrudex::ThrudexException> putList(const std::vector<thrudex::Document> &documents)
    {
      return this->get_backend ()->putList (documents);
    }

    virtual std::vector<thrudex::ThrudexException> removeList(const std::vector<thrudex::Element> &elements)
    {
      return this->get_backend ()->removeList (elements);
    }

    virtual std::vector<thrudex::SearchResponse> searchList(const std::vector<thrudex::SearchQuery> &q)
    {
      return this->get_backend ()->searchList (q);
    }

    virtual std::string admin(const std::string &op, const std::string &data)
    {
      return this->get_backend ()->admin (op, data);
    }
 protected:
    void set_backend (boost::shared_ptr<ThrudexBackend> backend)
    {
      this->backend = backend;
    }

    boost::shared_ptr<ThrudexBackend> get_backend ()
    {
      return this->backend;
    }

 private:
    boost::shared_ptr<ThrudexBackend> backend;
};

#endif /* _THRUDEX_PASSTHRU_BACKEND_H_ */


