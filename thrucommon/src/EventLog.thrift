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
    void log (1:Event event) throws (EventLogException e)
}
