#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include <stdio.h>

#if HAVE_CPPUNIT

#include "CircuitBreaker.h"

#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <log4cxx/propertyconfigurator.h>
#include <string>

using namespace std;
using namespace log4cxx;

class CircuitBreakerTest : public CppUnit::TestFixture
{
    public:
        CPPUNIT_TEST_SUITE (CircuitBreakerTest);
        CPPUNIT_TEST (testAll);
        CPPUNIT_TEST_SUITE_END ();

        void testAll ()
        {
            uint16_t threshold = 5;
            uint16_t timeout = 1;

            CircuitBreaker breaker (threshold, timeout);

            CPPUNIT_ASSERT (breaker.allow ());
            breaker.success ();

            // do 6 failures, we have to go above the threshold...

            bool allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 1
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 2
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 3
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 4
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 5
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // 6
            allow = breaker.allow ();
            CPPUNIT_ASSERT (!allow);

            // now sleep more than timeout, we should be allowed, in half-open
            sleep (timeout + 1);
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.failure ();

            // we failed in half-open should imed go back to open
            allow = breaker.allow ();
            CPPUNIT_ASSERT (!allow);

            // now sleep more than timeout, we should be allowed, in half-open,
            // but this time we'll succeed
            sleep (timeout + 1);
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
            if (allow)
                breaker.success ();

            // we should now be back in closed
            allow = breaker.allow ();
            CPPUNIT_ASSERT (allow);
        }

    private:
        int all_callback_count;
        int sender_callback_count;
        int group_callback_count;
        int type_callback_count;
        int once_callback_count;

};

// Registers the fixture into the 'registry'
CPPUNIT_TEST_SUITE_REGISTRATION (CircuitBreakerTest);

int main (int /* argc */, char*[] /* argv */)
{
    // init log
    PropertyConfigurator::configure ("test.conf");

    // Get the top level suite from the registry
    CppUnit::Test *suite = CppUnit::TestFactoryRegistry::getRegistry ().makeTest ();

    // Adds the test to the list of test to run
    CppUnit::TextUi::TestRunner runner;
    runner.addTest (suite);

    // Change the default outputter to a compiler error format outputter
    runner.setOutputter (new CppUnit::CompilerOutputter (&runner.result (),
                                                         std::cerr));
    // Run the tests.
    bool wasSucessful = runner.run ();

    // Return error code 1 if the one of test failed.
    return wasSucessful ? 0 : 1;
}

#else

int main (int /* argc */, char*[] /* argv */)
{
    fprintf (stderr, "cppunit support not avaiable\n");
    return -1;
}

#endif /* HAVE_CPPUNIT */
