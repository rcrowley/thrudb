/*
 *
 */

#include <CLucene.h>

using namespace lucene::analysis::standard;
using namespace lucene::document;
using namespace lucene::index;
using namespace lucene::queryParser;
using namespace lucene::search;

wstring build_wstring( const string &str );

/*
 * TODO:
 * - lots of stuff
 * - the switching, cleaning code needs to be put in to functions, close, 
 *   delete, NULL stuff... so that it's not repeated everywhere
 * - put needs to support the idea of update, jake points out that it might be
 *   a good use of his favorite toy bloom filters :) so long as removes of 
 *   things that don't exist are only an expense and not a problem it should 
 *   work. but will need to rebuild from time to time etc. and might get ugly.
 */

class CLuceneBackend
{
    public:
        CLuceneBackend (string index_path);
        ~CLuceneBackend ();

        // these won't be CLucene types in the real deal
        void put (const Document * document);
        void remove (const string & id);
        vector<string> query (const string & query, bool stale_ok=true);

    private:
        string index_path;
        StandardAnalyzer analyzer;
        IndexWriter * writer;
        IndexReader * reader;
        Searcher * searcher;
        bool modified_since_searcher;
};

CLuceneBackend::CLuceneBackend (string index_path)
{
    this->index_path = index_path;
    this->writer = NULL;
    this->reader = NULL;
    this->searcher = NULL;
    this->modified_since_searcher = false;

    if (IndexReader::indexExists (index_path.c_str ())) 
    {
        if (IndexReader::isLocked (index_path.c_str ()))
            IndexReader::unlock (index_path.c_str ());
    }
    else
    {
        // we're going to open it to create, might as well leave it open and
        // use it.
        this->writer = new IndexWriter (index_path.c_str (), &analyzer, true, 
                                        true);
        fprintf (stderr, "CLuceneBackend: opening writer\n");
    }
}

CLuceneBackend::~CLuceneBackend ()
{
    if (this->writer)
    {
        fprintf (stderr, "~CLuceneBackend: closing writer\n");
        this->writer->close ();
        delete this->writer;
    }
    if (this->reader)
    {
        fprintf (stderr, "~CLuceneBackend: closing reader\n");
        this->reader->close ();
        delete this->reader;
    }
    if (this->searcher)
    {
        fprintf (stderr, "~CLuceneBackend: closing searcher\n");
        this->searcher->close ();
        delete this->searcher;
    }
}

void CLuceneBackend::put (const Document * document)
{
    // mutex

    // gonna require a writer
    if (!this->writer)
    {
        fprintf (stderr, "put: need writer\n");
        if (this->reader)
        {
            fprintf (stderr, "put: closing reader\n");
            this->reader->close ();
            delete this->reader;
            this->reader = NULL;
        }
        this->writer = new IndexWriter (index_path.c_str (), 
                                        &this->analyzer, false);
    }

    // TODO: does addDocument modify the document?
    this->writer->addDocument ((Document *)document);
    this->modified_since_searcher = true;
}

void CLuceneBackend::remove (const string & id)
{
    // mutex

    // gonna require a reader
    if (!this->reader)
    {
        fprintf (stderr, "remove: need reader\n");
        if (this->writer)
        {
            fprintf (stderr, "remove: closing writer\n");
            this->writer->close ();
            delete this->writer;
            this->writer = NULL;
        }
        this->reader = IndexReader::open (this->index_path.c_str ());
    }

    // TODO: does deleteTerm modify the term?
    Term * term = new Term ( _T("id"), build_wstring (id).c_str ());
    this->reader->deleteTerm (term);
    this->modified_since_searcher = true;
}

vector<string> CLuceneBackend::query (const string & query, bool stale_ok)
{
    // gonna requre a searcher
    if (!this->searcher)
    {
        fprintf (stderr, "query: need searcher\n");
        // mutex, also making sure that we don't double do it...
        this->searcher = new IndexSearcher (this->index_path.c_str ());
    }
    else if (!stale_ok && this->modified_since_searcher)
    {
        fprintf (stderr, "query: need new searcher\n");
        // mutex, also making sure that we don't double do it..., complicated
        // by the fact that calls could end up resetting 
        // modified_since_searcher...
        if (this->reader)
        {
            // close out reader so we'll get it's updates
            this->reader->close ();
            delete this->reader;
            this->reader = NULL;
        }
        else if (this->writer)
        {
            // close out writer so we'll get it's updates
            this->writer->close ();
            delete this->writer;
            this->writer = NULL;
        }
        if (this->searcher)
        {
            // close out searcher b/c we're going to recreate it
            this->searcher->close ();
            delete this->searcher;
        }
        this->searcher = new IndexSearcher (this->index_path.c_str ());
        this->modified_since_searcher = false;
    }

    vector<string> ids;
    Query * _query = QueryParser::parse (build_wstring (query).c_str (), L"id", 
                                         &this->analyzer);
    Hits * hits = this->searcher->search (_query);
    // no offset/counts for now...
    char buf[1024];
    for (int i = 0; i < hits->length (); i++)
    {
        Document * doc = &hits->doc (i);
        const wchar_t * id   = doc->get(_T("id"));
        STRCPY_TtoA (buf, id, 1024);
        ids.push_back (string (buf));
        // TODO: loads of memory freeing and shit
    }

    return ids;
}

int main (int argc, char * argv[])
{
    try
    {
        CLuceneBackend backend ("/home/rm/lib/thrudb/trunk/thrudex/index/play");

        // write
        Document document;
        Field * field = Field::Keyword (L"id", L"1");
        document.add (*field);
        field = Field::Keyword (L"query", L"one two three four");
        document.add (*field);
        backend.put (&document);

        // query
        string query = "id:1";
        vector<string> ids = backend.query (query);
        fprintf (stderr, "query init: length=%d\n", (int)ids.size ());
        ids = backend.query (query, false);
        fprintf (stderr, "query init: length=%d\n", (int)ids.size ());

        // remove
        backend.remove ("1");

        // query
        ids = backend.query (query);
        fprintf (stderr, "query init: length=%d\n", (int)ids.size ());
        ids = backend.query (query, false);
        fprintf (stderr, "query init: length=%d\n", (int)ids.size ());
    }
    catch (CLuceneError e)
    {
        fprintf (stderr, "CLuceneError: what=%s\n", e.what ());
        throw e;
    }
    return 0;
}

wstring build_wstring( const string &str )
{
    wstring wtmp;

    wchar_t    tmp_wchar;
    int        nchar;
    mbstate_t  ps;
    const char *c_str = str.c_str();
    size_t size       = str.size();

    memset(&ps, 0, sizeof(ps));
    wtmp = L"";

    while (c_str && size) {
        nchar = mbrtowc(&tmp_wchar, c_str, size, &ps);

        if (nchar == -1) {
            c_str++;
            size--;
            continue;
        }

        //signals end of str
        if (nchar == 0)
            break;

        wtmp    += tmp_wchar;
        c_str   += nchar;
        size    -= nchar;
    }

    return wtmp;
}

#if 0 
Searcher * get_searcher (string idx_path)
{
    Searcher * index_searcher = new IndexSearcher (idx_path.c_str ());
    Searchable * sub_searchers[2] = { index_searcher, NULL };
    MultiSearcher * multi_searcher = new MultiSearcher (sub_searchers);
    return multi_searcher;
}
#endif
