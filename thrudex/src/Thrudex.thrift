cpp_namespace thrudex
php_namespace Thrudex
perl_package  Thrudex
java_package  thrudex


exception ThrudexException
{
        1: string what
}

enum FieldType
{
        KEYWORD  = 1,  #Fixed string, not analyzed
        TEXT     = 2   #Analyzed text
}

struct Field
{
        1: string    key,
        2: string    value,

        3: FieldType type       = TEXT,
        4: i32       weight     = 1,
        5: bool      sortable   = 0
}

struct Document
{
        1: string      index
        2: string      key,
        3: list<Field> fields,
        4: i32         weight = 1
}

struct Element
{
        1:string index,
        2:string key
}

struct SearchQuery
{
        1: string  index,

        2: string  query,
        3: string  sortby,

        4: i32     limit     = 10,
        5: i32     offset    = 0,

        6: bool    desc      = 0,
        7: bool    randomize = 0
}

struct SearchResponse
{
        1: i32              total = -1,   #total across the entire index
        2: list<Element>    elements,
        3: ThrudexException ex
}

service Thrudex
{
        void           ping(),
        list<string>   getIndices(),

        void           put   (1:Document d)       throws(ThrudexException e),
        void           remove(1:Element  e)       throws(ThrudexException e),
        SearchResponse search(1:SearchQuery s)    throws(ThrudexException e),

        list<ThrudexException>  putList   (1:list<Document> documents) throws(ThrudexException e),
        list<ThrudexException>  removeList(1:list<Element>  elements)  throws(ThrudexException e),
        list<SearchResponse>    searchList(1:list<SearchQuery>     q)  throws(ThrudexException e)


        # the following is protected api, it us only to be used by administrative
        # programs and people who really know what they're doing.
        string         admin(1:string op, 2:string data)          throws(ThrudexException e)
}
