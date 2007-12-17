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
    2: string value
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

    # scan can be used to walk over all of the elements in a parition in an
    # undefined order. it is also only garunteed to pick up the elements that
    # exist at the time of the first call to scan. new elements _may_ be picked
    # up.
    ScanResponse scan(1:string tablename, 2:string seed, 3:i32 count)
}
