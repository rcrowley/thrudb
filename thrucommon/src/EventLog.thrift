exception EventLogException
{
    1: string what
}

struct Event
{
    1:i64    timestamp,
    2:string message
}

service EventLog
{
    void log (1:Event event) throws (EventLogException e),
    void nextLog (1:string next_filename) throws (EventLogException e)
}

struct Message
{
    1:string uuid,
    2:string sender,
    3:string method,
    4:map<string, string> params
}
