#ifdef HAVE_CONFIG_H
#include "thrudex_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include "CLuceneBackend.h"
#include "utils.h"
#include <boost/filesystem.hpp>
#include <thrift/concurrency/Util.h>

using namespace thrudex;

using namespace log4cxx;
using namespace boost;
using namespace facebook::thrift::concurrency;

LoggerPtr CLuceneBackend::logger (Logger::getLogger ("CLuceneBackend"));

CLuceneBackend::CLuceneBackend(const string &idx_root)
    : idx_root(idx_root)
{
    //Verify log dir
    if(!directory_exists( idx_root )){
        LOG4CXX_ERROR(logger,"Invalid index root: "+idx_root);
        throw runtime_error("Invalid index root: "+idx_root);
    }


    analyzer = boost::shared_ptr<lucene::analysis::Analyzer>(new lucene::analysis::standard::StandardAnalyzer());

    //grab the list of current indices
    boost::filesystem::directory_iterator i(idx_root);
    boost::filesystem::directory_iterator end;

    if (i != end && is_directory (i->status ()))
    {
        this->addIndex( i->path().leaf() );
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
    //Is index already loaded?
    if(this->isValidIndex(index))
        return;

    RWGuard g(mutex,true);

    //Is index already loaded? (double check)
    if(this->isValidIndex(index))
        return;

    index_cache[index] =
        shared_ptr<CLuceneIndex>(new CLuceneIndex(idx_root,index,analyzer));
}


void CLuceneBackend::put(const thrudex::Document &d)
{
    if(!this->isValidIndex( d.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+d.index;

        throw ex;
    }


    lucene::document::Document *doc = new lucene::document::Document();

    LOG4CXX_DEBUG(logger,"put: "+d.key);

    try{

        wstring doc_key     = build_wstring( d.key );
        assert( !doc_key.empty() );


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
   if(!this->isValidIndex( el.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+el.index;

        throw ex;
   }

   index_cache[el.index]->remove(el.key);
}


void CLuceneBackend::search(const thrudex::SearchQuery &q, thrudex::SearchResponse &r)
{
    if(!this->isValidIndex( q.index )){
        ThrudexException ex;
        ex.what = "Invalid index: "+q.index;

        throw ex;
    }

    index_cache[q.index]->search(q,r);
}


string CLuceneBackend::admin(const std::string &op, const std::string &data)
{
    if(op == "create_index"){

        string name = data;

        for(int i=0; i<name.size(); i++){
            if( !isascii(name[i]) )
                name[i] = '_';


            if(name[i] == '\t' || name[i] == '|' || name[i] == '\n' || name[i] == '.' ||
               name[i] == '\\' || name[i] == '/' )
                name[i] = '_';

            name[i] = tolower(name[i]);
        }


        this->addIndex(name);

        return "ok";
    }

    return "";
}
