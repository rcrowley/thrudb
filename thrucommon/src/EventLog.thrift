exception EventLogException
{
    1: string what
}

struct Event
{
    # TODO: would be nice if this was uint64_t...
    1:i64    timestamp,
    2:string message
}

service EventLog
{
    void log (1:Event event) throws (EventLogException e),
    void nextLog (1:string next_filename) throws (EventLogException e)
}
