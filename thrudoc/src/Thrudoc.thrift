namespace cpp thrudoc
php_namespace Thrudoc
perl_package  Thrudoc
namespace java thrudoc
ruby_namespace Thrudoc

enum ExceptionType {
     UNKNOWN     = 1,
     NO_SUCH_KEY = 2
}

exception ThrudocException
{
    1: string        what
    2: ExceptionType type = UNKNOWN
}

struct Element
{
    1: string bucket,
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
    2: ThrudocException ex
}

service Thrudoc
{
    list<string> getBuckets ()                                        throws(ThrudocException e),

    void         put(1:string bucket, 2:string key, 3:string value)   throws(ThrudocException e),
    string       get(1:string bucket, 2:string key)                   throws(ThrudocException e),
    void         remove(1:string bucket, 2:string key)                throws(ThrudocException e),

    # if you want thrudoc to generate a doc id for you
    string       putValue(1:string bucket, 2:string value)            throws(ThrudocException e),

    # scan can be used to walk over all of the elements in a partition in an
    # undefined order. it is also only guaranteed to pick up the elements that
    # exist at the time of the first call to scan. new elements _may_ be picked
    # up. a return of elements less than count means you've hit the end, this
    # includes 0 elements
    ScanResponse scan(1:string bucket, 2:string seed, 3:i32 count)    throws(ThrudocException e),

    # batch operations
    list<ThrudocException>   putList(1:list<Element> elements)        throws(ThrudocException e),
    list<ListResponse>         getList(1:list<Element> elements)      throws(ThrudocException e),
    list<ThrudocException>   removeList(1:list<Element> elements)     throws(ThrudocException e),
    list<ListResponse>         putValueList(1:list<Element> elements) throws(ThrudocException e),

    # the following is protected api, it us only to be used by administrative
    # programs and people who really know what they're doing.
    string admin(1:string op, 2:string data)                          throws(ThrudocException e)
}
