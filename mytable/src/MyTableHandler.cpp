
#include "MyTableHandler.h"

using namespace std;
using namespace log4cxx;

LoggerPtr MyTableHandler::logger (Logger::getLogger ("MyTableHandler"));

MyTableHandler::MyTableHandler (boost::shared_ptr<MyTableBackend> backend)
{
    this->backend = backend;
}
void MyTableHandler::put(const string & tablename, const string & key, const string & value)
{
    LOG4CXX_ERROR (logger, "put: tablename=" + tablename + ", key=" + key +
                   ", value=" + value);
    this->backend->put (tablename, key, value);
}

void MyTableHandler::get(string & _return, const string & tablename, const string & key)
{
    LOG4CXX_ERROR (logger, "get: tablename=" + tablename + ", key=" + key);
    _return = this->backend->get (tablename, key);
}

void MyTableHandler::remove(const string & tablename, const string & key)
{
    LOG4CXX_ERROR (logger, "remove: tablename=" + tablename + ", key=" + key);
    this->backend->remove (tablename, key);
}

void MyTableHandler::scan (vector<string> & _return, const string & tablename, const string & seed, int32_t count)
{
    LOG4CXX_ERROR (logger, "scan: tablename=" + tablename + ", seed=" + seed +
                   ", count=TODO");
    _return = this->backend->scan (tablename, seed, count);
}

