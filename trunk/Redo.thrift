struct RedoMessage
{
        1:i32     timestamp,
        2:string  transaction_id,
        3:string  message
}

service Redo
{
        void      log(1:RedoMessage m)
}

