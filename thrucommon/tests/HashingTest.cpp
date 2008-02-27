
#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include <stdio.h>

#if HAVE_CPPUNIT

#include "Hashing.h"

#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <log4cxx/propertyconfigurator.h>
#include <string>

using namespace std;
using namespace log4cxx;

class HashingTest : public CppUnit::TestFixture
{
    public:
        CPPUNIT_TEST_SUITE (HashingTest);
        CPPUNIT_TEST (testFNV32Hashing);
        CPPUNIT_TEST_SUITE_END ();

        void testFNV32Hashing ()
        {
            FNV32Hashing * hashing = new FNV32Hashing ();
            testHelper (hashing, "some key", 0.69562662);
            delete hashing;
        }

    private:

        void testHelper (Hashing * hashing, string key, double expected_value)
        {
            double first = hashing->get_point (key);
            CPPUNIT_ASSERT (0 <= first && first <= 1);
            double second = hashing->get_point (key);
            CPPUNIT_ASSERT_DOUBLES_EQUAL (first, second, 0.00000001);
            // make sure the value doesn't change over time
            CPPUNIT_ASSERT_DOUBLES_EQUAL (first, expected_value, 0.00000001);

            // try out two other, random keys

            key = "some1";
            first = hashing->get_point (key);
            CPPUNIT_ASSERT (0 <= first && first <= 1);
            second = hashing->get_point (key);
            CPPUNIT_ASSERT_DOUBLES_EQUAL (first, second, 0.00000001);

            key = "this is a much longer and somewhat %!@#$@# odd key";
            first = hashing->get_point (key);
            CPPUNIT_ASSERT (0 <= first && first <= 1);
            second = hashing->get_point (key);
            CPPUNIT_ASSERT_DOUBLES_EQUAL (first, second, 0.00000001);

        }

};

// Registers the fixture into the 'registry'
CPPUNIT_TEST_SUITE_REGISTRATION (HashingTest);

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
