cpp_namespace thrudoc
perl_package  Thrudoc

exception ThrudocException
{
        2: string what
}


service Thrudoc
{
        void         ping(),
        string       store(1:string obj, 2:string id) throws(ThrudocException e),
        bool         remove(1:string id)            throws(ThrudocException e),
        string       fetch(1:string id),

        list<string> fetchIds(1:i32 offset, 2:i32 limit),

        bool         removeList(1:list<string> ids) throws(ThrudocException e),
        list<string> fetchList(1:list<string> ids)
}


