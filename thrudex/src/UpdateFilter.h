#ifndef __UPDATE_FILTER_H__
#define __UPDATE_FILTER_H__

#include <CLucene.h>
#include <CLucene/search/Filter.h>
#include <boost/shared_ptr.hpp>
#include <string>

class UpdateFilter : public lucene::search::Filter
{
 public:
    UpdateFilter(boost::shared_ptr<lucene::index::IndexReader> reader);
    ~UpdateFilter();

    lucene::util::BitSet* bits(lucene::index::IndexReader* reader);

    lucene::search::Filter* clone() const;

    bool shouldDeleteBitSet(const lucene::util::BitSet* bs) const;

    void skip( std::wstring key );


    TCHAR* toString();


 private:
    boost::shared_ptr<lucene::util::BitSet>       bitset;
    boost::shared_ptr<lucene::index::IndexReader> reader;
    int32_t num_skipped;
};

#endif
