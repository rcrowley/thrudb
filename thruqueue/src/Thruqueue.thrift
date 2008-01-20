cpp_namespace thruqueue
php_namespace Thruqueue
perl_package  Thruqueue
java_package  thruqueue

exception ThruqueueException
{
        1: string what
}

service Thruqueue
{
        void         ping(),
        void         create (1:string queue_name, 2:bool unique = 1),
        void         destroy(1:string queue_name),
        void         enqueue(1:string queue_name, 2:string mess, 3:bool priority = 0),
        string       dequeue(1:string queue_name),
        void         flush  (1:string queue_name),
        string       peek   (1:string queue_name),
        i32          length (1:string queue_name)
}


enum   QueueOp
{
        UNKNOWN = 0,
        ENQUEUE = 1,
        DEQUEUE = 2,
        FLUSH   = 3
}

struct QueueMessage
{
        1:string     message_id,
        2:string     key,
        3:string     message,
        4:QueueOp    op = UNKNOWN,
        5:i32        timestamp
}

service QueueLog
{
        async void log(1:QueueMessage m)
}
