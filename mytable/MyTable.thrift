cpp_namespace mytable
php_namespace MyTable
perl_package  MyTable
java_package  MyTable

exception MyTableException
{
    2: string what
}

struct Element
{
    1: string key,
    2: string value,
#   TODO: add these in, return them in scan and maybe add getFull or something
#    3: i64    created_at,
#    4: i64    modified_at
}

struct ScanResponse
{
    1: list<Element> elements,
    2: string        seed
}

service MyTable
{
    void         put(1:string tablename, 2:string key, 3:string value) throws(MyTableException e),
    string       get(1:string tablename, 2:string key)                 throws(MyTableException e),
    void         remove(1:string tablename, 2:string key)              throws(MyTableException e),

    # scan can be used to walk over all of the elements in a partition in an
    # undefined order. it is also only guaranteed to pick up the elements that
    # exist at the time of the first call to scan. new elements _may_ be picked
    # up. a return of elements less than count means you've hit the end, this
    # includes 0 elements
    ScanResponse scan(1:string tablename, 2:string seed, 3:i32 count)
}
