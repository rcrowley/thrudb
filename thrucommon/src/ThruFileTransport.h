
#ifndef _MYFILETRANSPORT_H_
#define _MYFILETRANSPORT_H_ 1

#include "transport/TTransport.h"
#include "Thrift.h"
#include "TProcessor.h"

#include <boost/shared_ptr.hpp>
#include <log4cxx/logger.h>
#include <stdio.h>
#include <string>
#include <thrift/concurrency/Mutex.h>

class ThruFileReaderTransport : public facebook::thrift::transport::TTransport
{
    public:
        ThruFileReaderTransport(std::string path);
        ~ThruFileReaderTransport();

        uint32_t read(uint8_t* buf, uint32_t len) ;

    private:
        static log4cxx::LoggerPtr logger;

        std::string path;
        int fd;
};

class ThruFileWriterTransport : public facebook::thrift::transport::TTransport
{
    public:
        ThruFileWriterTransport(std::string path, uint32_t sync_wait);
        ~ThruFileWriterTransport();

        void write(const uint8_t* buf, uint32_t len);

    private:
        static log4cxx::LoggerPtr logger;

        std::string path;
        int fd;
        pthread_t fsync_thread;
        uint32_t sync_wait;
        facebook::thrift::concurrency::Mutex write_mutex;

        static void * start_sync_thread (void * ptr);
        void fsync_thread_run ();
};

class ThruFileProcessor 
{
    public:
        /** 
         * Constructor that defaults output transport to null transport
         * 
         * @param processor processes log-file events
         * @param protocolFactory protocol factory
         * @param inputTransport file transport
         */
        ThruFileProcessor(boost::shared_ptr<facebook::thrift::TProcessor> processor,
                        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> protocolFactory,
                        boost::shared_ptr<ThruFileReaderTransport> inputTransport);

        ThruFileProcessor(boost::shared_ptr<facebook::thrift::TProcessor> processor,
                        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> inputProtocolFactory,
                        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> outputProtocolFactory,
                        boost::shared_ptr<ThruFileReaderTransport> inputTransport);

        /** 
         * Constructor
         * 
         * @param processor processes log-file events
         * @param protocolFactory protocol factory
         * @param inputTransport input file transport
         * @param output output transport
         */    
        ThruFileProcessor(boost::shared_ptr<facebook::thrift::TProcessor> processor,
                        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> protocolFactory,
                        boost::shared_ptr<ThruFileReaderTransport> inputTransport,
                        boost::shared_ptr<facebook::thrift::transport::TTransport> outputTransport);

        /**
         * processes events from the file
         *
         * @param numEvents number of events to process (0 for unlimited)
         * @param tail tails the file if true
         */
        void process(uint32_t numEvents);

    private:
        static log4cxx::LoggerPtr logger;

        boost::shared_ptr<facebook::thrift::TProcessor> processor_;
        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> inputProtocolFactory_;
        boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> outputProtocolFactory_;
        boost::shared_ptr<ThruFileReaderTransport> inputTransport_;
        boost::shared_ptr<facebook::thrift::transport::TTransport> outputTransport_;
};

#endif // _THRIFT_TRANSPORT_TFILETRANSPORT_H_
