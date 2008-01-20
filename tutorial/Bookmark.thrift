perl_package  Tutorial
php_namespace Tutorial
java_package  tutorial

#
#Define bookmarks in thrift
#
struct Bookmark
{
    1: string title,
    2: string url,
    3: i32    posted_date,
    4: string tags
}

