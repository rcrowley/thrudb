#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "CLuceneBackend.h"
#include "utils.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <thrift/concurrency/Util.h>

namespace fs = boost::filesystem;
using namespace boost;
using namespace facebook::thrift::concurrency;
using namespace log4cxx;
using namespace thrudex;

LoggerPtr CLuceneBackend::logger (Logger::getLogger ("CLuceneBackend"));

CLuceneBackend::CLuceneBackend(const string &idx_root)
    : idx_root(idx_root)
{
    LOG4CXX_INFO( logger, "CLuceneBackend: idx_root=" + idx_root );
    //Verify log dir
    if(!directory_exists( idx_root )){
        fs::create_directories( idx_root );
    }

    analyzer = boost::shared_ptr<lucene::analysis::Analyzer>(new lucene::analysis::standard::StandardAnalyzer());

    //grab the list of current indices
    boost::filesystem::directory_iterator end;

    for(boost::filesystem::directory_iterator i(idx_root); i != end; ++i){

        if(is_directory (i->status ()))
        {
            //skip hidden dirs (.)
            if(i->path().leaf().substr(0,1) == ".")
                continue;

            this->addIndex( i->path().leaf() );

            LOG4CXX_INFO(logger, "Added "+i->path().leaf() );
        }
    }
}

CLuceneBackend::~CLuceneBackend()
{

}

bool CLuceneBackend::isValidIndex(const string &index)
{
    RWGuard g(mutex);

    return (index_cache.count(index) ? true : false);
}


vector<string> CLuceneBackend::getIndices()
{
    LOG4CXX_DEBUG( logger, "getIndices:" );

    RWGuard g(mutex);

    vector<string> indices;

    map<string,shared_ptr<CLuceneIndex> >::iterator it;

    for(it = index_cache.begin(); it != index_cache.end(); ++it){
        indices.push_back(it->first);
    }

    return indices;
}

void CLuceneBackend::addIndex(const string &index)
{
    LOG4CXX_DEBUG( logger, "addIndex: index=" + index );

    //Is index already loaded?
    if(this->isValidIndex(index))
        return;

    index_cache[index] =
        shared_ptr<CLuceneIndex>(new CLuceneIndex(idx_root,index,analyzer));
}


void CLuceneBackend::put(const thrudex::Document &d)
{
    LOG4CXX_DEBUG( logger, "put: d.index=" + d.index + ", d.key=" + d.key );

    if(!this->isValidIndex( d.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+d.index;

        throw ex;
    }


    lucene::document::Document *doc = new lucene::document::Document();

    try{

        wstring doc_key     = build_wstring( d.key );

        if( doc_key.empty() ){
            ThrudexException ex;
            ex.what = "Missing key";

            throw ex;
        }


        lucene::document::Field *f;

        f = lucene::document::Field::Keyword( DOC_KEY, doc_key.c_str() );

        doc->add(*f);

        for( unsigned int j=0; j<d.fields.size(); j++){

            LOG4CXX_DEBUG(logger, d.fields[j].key+":"+d.fields[j].value);

            wstring key   = build_wstring( d.fields[j].key   );
            wstring value = build_wstring( d.fields[j].value );


            switch(d.fields[j].type){
                case thrudex::KEYWORD:
                    LOG4CXX_DEBUG(logger,"Keyword");
                    f = lucene::document::Field::Keyword(key.c_str(),value.c_str());  break;
                case thrudex::TEXT:
                    LOG4CXX_DEBUG(logger,"Text");
                    f = lucene::document::Field::Text(key.c_str(),value.c_str());     break;
                default:
                    LOG4CXX_DEBUG(logger,"UnStored");
                    f = lucene::document::Field::UnStored(key.c_str(),value.c_str()); break;
            };

            if(d.fields[j].weight> 0){
                f->setBoost( d.fields[j].weight );
            }


            doc->add(*f);

            //If Sorted field then add _sort
            if(d.fields[j].sortable){
                key += L"_sort";

                f = lucene::document::Field::Keyword(key.c_str(),value.c_str());

                doc->add(*f);
            }
        }

        if(d.weight > 0)
            doc->setBoost(d.weight);


        index_cache[d.index]->put( d.key, doc );

    }catch(...){
        //
    }

    delete doc;
}

void CLuceneBackend::remove(const thrudex::Element &el)
{
    LOG4CXX_DEBUG( logger, "remove: el.index=" + el.index + ", el.key=" +
                   el.key );

    if(!this->isValidIndex( el.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+el.index;

        throw ex;
    }

    index_cache[el.index]->remove(el.key);
}


void CLuceneBackend::search(const thrudex::SearchQuery &q, thrudex::SearchResponse &r)
{
    LOG4CXX_DEBUG( logger, "search: q.index=" + q.index + ", q.query=" +
                   q.query );

    if(!this->isValidIndex( q.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+q.index;

        throw ex;
    }

    index_cache[q.index]->search(q,r);
}


string CLuceneBackend::admin(const std::string &op, const std::string &data)
{
    LOG4CXX_DEBUG( logger, "admin: op=" + op + ", data=" + data );

    string log_pos_file = idx_root + "/thrudex.state";

    if(op == "create_index"){

        string name = data;

        for(unsigned int i=0; i<name.size(); i++){
            if( !isascii(name[i]) ){
                ThrudexException ex;
                ex.what = "Index name contains non-ascii chars";
                throw ex;
            }


            if(name[i] == '\t' || name[i] == '|' || name[i] == '\n' || name[i] == '.' ||
               name[i] == '\\' || name[i] == '/' ){

                ThrudexException ex;
                ex.what = "Index name contains illegal chars";
                throw ex;
            }


        }

        LOG4CXX_INFO(logger, "Creating index:"+name);
        this->addIndex(name);

        return "ok";
    } else if (op == "put_log_position") {
        fs::ofstream outfile;
        outfile.open( log_pos_file.c_str (), 
                      ios::out | ios::binary | ios::trunc);
        if (!outfile.is_open ())
        {
            ThrudexException e;
            e.what = "can't open log posotion file=" + log_pos_file;
            LOG4CXX_ERROR( logger, e.what);
            throw e;
        }
        outfile.write (data.data (), data.size ());
        outfile.close();
        return "done";
    } else if (op == "get_log_position") {
        fs::ifstream infile;
        infile.open (log_pos_file.c_str (), ios::in | ios::binary | ios::ate);
        if (!infile.is_open ())
        {
            ThrudexException e;
            e.what = "can't open log posotion file=" + log_pos_file;
            LOG4CXX_ERROR( logger, e.what);
            throw e;
        }
        fs::ifstream::pos_type size = infile.tellg ();
        char * memblock = new char [size];

        infile.seekg (0, ios::beg);
        infile.read (memblock, size);

        infile.close ();

        string obj (memblock, size);

        delete [] memblock;

        return obj;
    }

    return "";
}
