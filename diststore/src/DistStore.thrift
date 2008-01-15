cpp_namespace diststore
php_namespace DistStore
perl_package  DistStore
java_package  DistStore

exception DistStoreException
{
    1: string what
}

struct Element
{
    1: string tablename,
    2: string key,
    3: string value
}

struct ScanResponse
{
    1: list<Element> elements,
    2: string        seed
}

struct ListResponse
{
    1: Element            element,
    2: DistStoreException ex
}

service DistStore
{
    list<string> getTablenames ()                                      throws(DistStoreException e),

    void         put(1:string tablename, 2:string key, 3:string value) throws(DistStoreException e),
    string       get(1:string tablename, 2:string key)                 throws(DistStoreException e),
    void         remove(1:string tablename, 2:string key)              throws(DistStoreException e),

    # if you want diststore to generate a doc id for you
    string       putValue(1:string tablename, 2:string value)          throws(DistStoreException e),

    # scan can be used to walk over all of the elements in a partition in an
    # undefined order. it is also only guaranteed to pick up the elements that
    # exist at the time of the first call to scan. new elements _may_ be picked
    # up. a return of elements less than count means you've hit the end, this
    # includes 0 elements
    ScanResponse scan(1:string tablename, 2:string seed, 3:i32 count)  throws(DistStoreException e),

    # batch operations
    list<DistStoreException>   putList(1:list<Element> elements)       throws(DistStoreException e),
    list<ListResponse>         getList(1:list<Element> elements)       throws(DistStoreException e),
    list<DistStoreException>   removeList(1:list<Element> elements)    throws(DistStoreException e),
    list<ListResponse>         putValueList(1:list<Element> elements)  throws(DistStoreException e),

    # the following is protected api, it us only to be used by administrative
    # programs and people who really know what they're doing.
    string admin(1:string op, 2:string data) throws(DistStoreException e)
}
