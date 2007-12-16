cpp_namespace mytable
php_namespace MyTable
perl_package  MyTable
java_package  MyTable

exception MyTableException
{
    2: string what
}

struct ScanResults
{
    1: list<string> ids,
    2: string       seed
}

service MyTable
{
    void         put(1:string tablename, 2:string key, 3:string value) throws(MyTableException e),
    string       get(1:string tablename, 2:string key)                 throws(MyTableException e),
    void         remove(1:string tablename, 2:string key)               throws(MyTableException e),

    list<string> scan(1:string tablename, 2:string seed, 3:i32 count)
}
