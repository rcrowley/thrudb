cpp_namespace thrudex
php_namespace Thrudex
perl_package  Thrudex
java_package  Thrudex

enum ExceptionClass
{
        WARNING,
        CRITICAL
}

exception ThrudexException
{
        1: ExceptionClass eclass,
        2: string what
}



struct QueryMsg
{
        1: string  domain,
        2: string  index,
        3: i32     limit = 200,
        4: i32     offset= 0,
        5: string  sortby,
        6: bool    desc = 0,
        7: bool    randomize = 0,
        8: string  query
}

struct QueryResponse
{
        1: i32          total,
        2: list<string> ids
}

struct RemoveMsg
{
        1: string domain,
        2: string index,
        3: string docid
}

enum StorageType
{
        KEYWORD  = 1,
        TEXT     = 2,
        UNSTORED = 3
}

struct Field
{
        1: string name,
        2: string value,
        3: StorageType stype = UNSTORED,
        4: i32    boost      = 1,
        5: bool   sortable   = 0
}

struct DocMsg
{
        1: string      domain,
        2: string      index,
        3: string      docid,
        4: i32         boost = 1,
        5: list<Field> fields
}

service Thrudex
{
        void          ping(),
        void          add(1:DocMsg d)       throws(ThrudexException e),
        void          update(1:DocMsg u)    throws(ThrudexException e),
        void          remove(1:RemoveMsg d) throws(ThrudexException e),
        QueryResponse query(1:QueryMsg q)   throws(ThrudexException e),

        void         addList(1:list<DocMsg> d)             throws(ThrudexException e),
        void         updateList(1:list<DocMsg> u)          throws(ThrudexException e),
        void         removeList(1:list<RemoveMsg> d)       throws(ThrudexException e),
        list<QueryResponse> queryList(1:list<QueryMsg> q)  throws(ThrudexException e)

        void         optimize(string domain, string index) throws(ThrudexException e);
        void         optimizeAll()                         throws(ThrudexException e);
        void         commit(string domain, string index)   throws(ThrudexException e);
        void         commitAll()                           throws(ThrudexException e);
}
