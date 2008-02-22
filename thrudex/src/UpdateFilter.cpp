#include "UpdateFilter.h"
#include "CLuceneIndex.h"

using namespace boost;
using namespace lucene::index;
using namespace lucene::search;
using namespace lucene::util;

UpdateFilter::UpdateFilter(shared_ptr<IndexReader> reader)
    : reader(reader), num_skipped(0)
{

    bitset = shared_ptr<BitSet>(new BitSet(reader->maxDoc()));

    //Set them all to true
    for(int32_t i=0; i<reader->maxDoc(); i++){
        bitset->set(i,true);
    }
}

UpdateFilter::~UpdateFilter()
{

}

BitSet* UpdateFilter::bits(IndexReader* reader)
{
    if(reader->maxDoc() == this->reader->maxDoc())
        return bitset.get();
    else
        return NULL;
}

void UpdateFilter::skip( wstring key )
{
    Term              *t = new Term(DOC_KEY, key.c_str() );
    TermEnum *enumerator = reader->terms(t);
    if (enumerator->term(false) == NULL){
        _CLDELETE(enumerator);
        delete t;
        return;
    }

    TermDocs* termDocs = reader->termDocs();

    try {

        termDocs->seek(enumerator->term(false));
        while (termDocs->next()) {
            bitset->set(termDocs->doc(),false);
        }

    } _CLFINALLY (
        termDocs->close();
        _CLDELETE(termDocs);
        enumerator->close();
        _CLDELETE(enumerator);
        delete t;
    );
}

Filter* UpdateFilter::clone() const
{
    return NULL;
}


bool UpdateFilter::shouldDeleteBitSet(const BitSet* bs) const
{
    return false;
}


TCHAR* UpdateFilter::toString()
{
    return L"";
}
