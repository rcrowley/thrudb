/**
 * Copyright (c) 2007- T Jake Luciani
 * Distributed under the New BSD Software License
 *
 * See accompanying file LICENSE or visit the Thrudb site at:
 * http://thrudb.googlecode.com
 *
 **/
#include "ThrudocS3SpreadTask.h"

#include "RecoveryManager.h"
#include "TransactionManager.h"
#include "Transaction.h"

#include <boost/shared_ptr.hpp>

#include "Thrudoc.h"
#include "ThrudocHandler.h"
#include "ThrudocSimpleS3Backend.h"
#include <thrift/concurrency/Thread.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

using namespace thrudoc;
using namespace facebook::thrift::concurrency;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;


ThrudocS3SpreadTask::ThrudocS3SpreadTask(boost::shared_ptr<Transaction> t)
        : transaction(t)
{

}

void ThrudocS3SpreadTask::run()
{
    //write threads happen locally
    TBinaryProtocolFactory protocol_factory;

    //thrudoc backend specific to spread
    boost::shared_ptr<ThrudocBackend>   backend  (new ThrudocSimpleS3Backend());
    boost::shared_ptr<ThrudocHandler>   handler  (new ThrudocHandler(backend));
    boost::shared_ptr<ThrudocProcessor> processor(new ThrudocProcessor(handler));

    const string raw_msg = transaction->getRawBuffer();

    boost::shared_ptr<TTransport> buf(new TMemoryBuffer( raw_msg ) );

    boost::shared_ptr<TProtocol> prot = protocol_factory.getProtocol(buf);

    try {
        processor->process(prot, prot);

    } catch (TTransportException& ttx) {
        //  cerr << "SpreadTask client died: " << ttx.what() << endl;
        throw;
    } catch (TException& x) {
        //  cerr << "SpreadTask exception: " << x.what() << endl;
        throw;
    } catch (...) {
        //  cerr << "SpreadTask uncaught exception." << endl;
        throw;
    }

    //Cleanup
    RecoveryManager->addRedo(raw_msg, transaction->getId());

    TransactionManager->endTransaction(transaction);
}
