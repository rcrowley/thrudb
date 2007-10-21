cpp_namespace thrucene
perl_package  Thrucene

enum ExceptionClass
{
        WARNING,
        CRITICAL
}

enum StorageType
{
        KEYWORD,
        TEXT,
        UNSTORED
}

exception ThruceneException
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

struct Field
{
        1: string name,
        2: string value,
        3: StorageType stype,
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

service Thrucene
{
        void         ping(),
        void         add(1:DocMsg d)       throws(ThruceneException e),
        void         update(1:DocMsg u)    throws(ThruceneException e),
        void         remove(1:RemoveMsg d) throws(ThruceneException e),
        QueryResponse query(1:QueryMsg q)   throws(ThruceneException e),

        void         addList(1:list<DocMsg> d)             throws(ThruceneException e),
        void         updateList(1:list<DocMsg> u)          throws(ThruceneException e),
        void         removeList(1:list<RemoveMsg> d)       throws(ThruceneException e),
        list<QueryResponse> queryList(1:list<QueryMsg> q)  throws(ThruceneException e)

        void         optimize(string domain, string index) throws(ThruceneException e);
        void         optimizeAll()                         throws(ThruceneException e);
        void         commit(string domain, string index)   throws(ThruceneException e);
        void         commitAll()                           throws(ThruceneException e);
}
