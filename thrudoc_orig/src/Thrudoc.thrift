cpp_namespace thrudoc
php_namespace Thrudoc
perl_package  Thrudoc
java_package  Thrudoc

exception ThrudocException
{
        2: string what
}

service Thrudoc
{
        #Make sure the service is up
        void         ping(),

        #
        #Add a new document to the store, it will return a identifier
        #
        string       add(1:string doc)              throws(ThrudocException e),
        list<string> addList(1:list<string> docs)   throws(ThrudocException e),



        #
        #Add or update a document with an identifier
        #
        void         store(1:string id, 2:string doc)     throws(ThrudocException e),
        void         storeList(1:map<string,string> docs) throws(ThrudocException e),

        bool         remove(1:string id)            throws(ThrudocException e),
        bool         removeList(1:list<string> ids) throws(ThrudocException e),

        string       fetch(1:string id),
        list<string> fetchList(1:list<string> ids),


        list<string> listIds(1:i32 offset, 2:i32 limit)
}


