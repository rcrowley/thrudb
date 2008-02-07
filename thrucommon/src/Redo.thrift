struct RedoMessage
{
        1:i32              timestamp,
        2:string           transaction_id,
        3:string           message
}

struct S3Message
{
        1:i32              timestamp,
        2:string           transaction_id
}

service Redo
{
        void      log(1:RedoMessage m),
        void      s3log(1:S3Message m)   #info to keep track of which items have been sent to s3
}
