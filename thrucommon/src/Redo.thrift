service Redo
{
        void      startup   (1:i64 timestamp),
        void      log       (1:i64 timestamp, 2:string transaction_id, 3:string log_message),
        void      commit    (1:i64 timestamp, 2:string transaction_id),
        void      fail      (1:i64 timestamp, 2:string transaction_id)
}

