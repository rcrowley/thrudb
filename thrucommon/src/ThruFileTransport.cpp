/* based on TFileTransport, but much simplier */

#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H 

#include "ThruFileTransport.h"
#include "transport/TTransportUtils.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <errno.h>

using namespace boost;
using namespace facebook::thrift;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;
using namespace log4cxx;
using namespace std;

// TODO: what about the case where we're destruting and get a write from
// another thread before that happens, not sure what we can do about it, might
// be up to the using code

LoggerPtr ThruFileReaderTransport::logger 
(Logger::getLogger ("ThruFileReaderTransport"));
LoggerPtr ThruFileWriterTransport::logger 
(Logger::getLogger ("ThruFileWriterTransport"));
LoggerPtr ThruFileProcessor::logger 
(Logger::getLogger ("ThruFileProcessor"));

ThruFileWriterTransport::ThruFileWriterTransport (string path, uint32_t sync_wait)
{
    // don't let people use us until we're done initing
    Guard g(write_mutex);

    char buf[512];
    sprintf (buf, "ThruFileWriterTransport: path=%s, sync_wait=%u\n",
             path.c_str (), sync_wait);
    LOG4CXX_INFO (logger, buf);
    this->path = path;
    this->sync_wait = sync_wait;

    errno = 0;
    this->fd = ::open (path.c_str (), O_RDWR | O_APPEND | O_CREAT, 
                       S_IRUSR | S_IWUSR| S_IRGRP | S_IROTH);
    if (this->fd == -1)
    {
        sprintf (buf, "ThruFileWriterTransport: problem opening log file (%s) - %s\n", 
                 path.c_str (), strerror (errno));
        LOG4CXX_ERROR (logger, buf);
        // TODO: convert to an exception
        fprintf (stderr, buf);
        exit (-1);
    }
    
    if (this->sync_wait > 0)
    {
        if (pthread_create(&fsync_thread, NULL, start_sync_thread, 
                           (void *)this) != 0) 
        {
            char buf[] = "ThruFileWriterTransport: start_sync_thread failed\n";
            LOG4CXX_ERROR (logger, buf);
            // TODO: convert to an exception
            fprintf (stderr, buf);
            exit (-1);
        }
    }
}

ThruFileWriterTransport::~ThruFileWriterTransport ()
{
    LOG4CXX_INFO (logger, "~ThruFileWriterTransport");
    // don't close things out from under other people
    Guard g(write_mutex);
    // close should sync...
    if (this->fd)
        ::close (this->fd);
    // setting fd to 0 will signal the writer thread, if any, to exit
    this->fd = 0;
    // TODO: i'd rather look to see if there's a fsync_thread
    if (this->sync_wait > 0)
        pthread_join(fsync_thread, NULL);
}

void ThruFileWriterTransport::write(const uint8_t* buf, uint32_t len)
{
    Guard g(write_mutex);
    uint32_t write_len;
    if ((write_len = ::write (this->fd, buf, len)) != len)
    {
        char buf[128];
        sprintf (buf, "write: problem writing %d bytes, wrote %d - %s\n",
                 len, write_len, strerror (errno));
        LOG4CXX_ERROR (logger, buf);
        // TODO: convert to an exception
        fprintf (stderr, buf);
        exit (-1);
    }
    if (this->sync_wait == 0)
#if HAVE_FDATASYNC
        fdatasync (this->fd);
#elif HAVE_FSYNC
        fsync (this->fd);
#else
#error "either fdatasync or fsync is required to use use ThruFileTransport"
#endif
}

void * ThruFileWriterTransport::start_sync_thread (void * ptr)
{
    LOG4CXX_INFO (logger, "start_sync_thread: ");
    (((ThruFileWriterTransport *)ptr)->fsync_thread_run ());
    return NULL;
}

void ThruFileWriterTransport::fsync_thread_run ()
{
    // once the fd goes away we'll end
    while (this->fd)
    {
        LOG4CXX_DEBUG (logger, "fsync_thread_run: ");
        {
            Guard g(write_mutex);
#if HAVE_FDATASYNC
            fdatasync (this->fd);
#elif HAVE_FSYNC
            fsync (this->fd);
#else
#error "either fdatasync or fsync is required to use use ThruFileTransport"
#endif
        }
        usleep (this->sync_wait);
    }
}

ThruFileReaderTransport::ThruFileReaderTransport (string path)
{
    LOG4CXX_INFO (logger, "ThruFileReaderTransport: path=" + path);
    this->path = path;
    errno = 0;
    this->fd = ::open (path.c_str (), O_RDONLY,
                       S_IRUSR | S_IWUSR| S_IRGRP | S_IROTH);
    if (this->fd == -1)
    {
        char buf[512];
        sprintf (buf, "ThruFileReaderTransport: problem opening log file (%s) - %s\n", 
                 path.c_str (), strerror (errno));
        LOG4CXX_ERROR (logger, buf);
        // TODO: convert to an exception
        fprintf (stderr, buf);
        exit (-1);
    }
}

ThruFileReaderTransport::~ThruFileReaderTransport ()
{
    LOG4CXX_INFO (logger, "~ThruFileReaderTransport");
    if (this->fd)
        ::close (this->fd);
}

uint32_t ThruFileReaderTransport::read(uint8_t* buf, uint32_t len) 
{
    int32_t read_len = 0;
    while (1)
    {
        errno = 0;
        read_len = ::read (this->fd, buf + read_len, len);
        if (read_len == (int32_t)len)
            break;
        else if (read_len == -1)
        {
            char buf[128];
            sprintf (buf, "error reading from file - %s\n", strerror (errno));
            LOG4CXX_ERROR (logger, buf);
            // TODO: convert to an exception
            fprintf (stderr, buf);
            exit (-1);
            break;
        }
        else /* we read 0 (most likely) or at least we still don't have len */
            usleep(1000000);
    }
    return (uint32_t)read_len;
}

ThruFileProcessor::ThruFileProcessor(shared_ptr<TProcessor> processor,
                                 shared_ptr<TProtocolFactory> protocolFactory,
                                 shared_ptr<ThruFileReaderTransport> inputTransport):
    processor_(processor),
    inputProtocolFactory_(protocolFactory),
    outputProtocolFactory_(protocolFactory),
    inputTransport_(inputTransport) 
{
    // default the output transport to a null transport (common case)
    outputTransport_ = shared_ptr<TNullTransport>(new TNullTransport());
}

ThruFileProcessor::ThruFileProcessor(shared_ptr<TProcessor> processor,
                                 shared_ptr<TProtocolFactory> inputProtocolFactory,
                                 shared_ptr<TProtocolFactory> outputProtocolFactory,
                                 shared_ptr<ThruFileReaderTransport> inputTransport):
    processor_(processor),
    inputProtocolFactory_(inputProtocolFactory),
    outputProtocolFactory_(outputProtocolFactory),
    inputTransport_(inputTransport) 
{
    // default the output transport to a null transport (common case)
    outputTransport_ = shared_ptr<TNullTransport>(new TNullTransport());
}

ThruFileProcessor::ThruFileProcessor(shared_ptr<TProcessor> processor,
                                 shared_ptr<TProtocolFactory> protocolFactory,
                                 shared_ptr<ThruFileReaderTransport> inputTransport,
                                 shared_ptr<TTransport> outputTransport):
    processor_(processor),
    inputProtocolFactory_(protocolFactory),
    outputProtocolFactory_(protocolFactory),
    inputTransport_(inputTransport),
    outputTransport_(outputTransport) 
{
};

void ThruFileProcessor::process(uint32_t numEvents) 
{
    shared_ptr<TProtocol> inputProtocol = inputProtocolFactory_->getProtocol(inputTransport_);
    shared_ptr<TProtocol> outputProtocol = outputProtocolFactory_->getProtocol(outputTransport_);

    uint32_t numProcessed = 0;
    while (1) 
    {
        // bad form to use exceptions for flow control but there is really
        // no other way around it
        try 
        {
            processor_->process(inputProtocol, outputProtocol);
            numProcessed++;
            if ((numEvents > 0) && (numProcessed == numEvents)) 
            {
                return;
            }
        }
        catch (TEOFException& teof) 
        {
            LOG4CXX_ERROR (logger, string ("process: TEOFException.what=") + 
                           teof.what ());
            break;
        } 
        catch (TException &te) 
        {
            LOG4CXX_ERROR (logger, string ("process: TException.what=") + 
                           te.what ());
            break;
        }
    }
}
