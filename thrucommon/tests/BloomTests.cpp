#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include <stdio.h>

#if HAVE_CPPUNIT

#include "bloom_filter.hpp"
#include "counting_bloom_filter.hpp"

#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <log4cxx/propertyconfigurator.h>
#include <string>

using namespace std;
using namespace log4cxx;


static char words[][10] = {"the","quick","brown","fox","jumped","over","the","lazy","dog"};
static int num_words= 9;


class BloomTests : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE (BloomTests);
    CPPUNIT_TEST (testBloom);
    CPPUNIT_TEST (testCountingBloom);
    CPPUNIT_TEST_SUITE_END ();

    void testBloom ()
    {
        unsigned int size = 100;

        bloom_filter *bf = new bloom_filter( size, 1.0/(1.0 * size), size*rand());

        CPPUNIT_ASSERT(bf);

        for(int i=0; i<num_words; i++){
            bf->insert( words[i] );
        }


        for(int i=0; i<num_words; i++){
            CPPUNIT_ASSERT_EQUAL (1, (int) bf->contains(words[i]));
        }

        delete bf;
    };


    void testCountingBloom ()
    {
        unsigned int size = 100;

        counting_bloom_filter *bf = new counting_bloom_filter( size, 1.0/(1.0 * size), size*rand());

        CPPUNIT_ASSERT(bf);

        //add
        for(int i=0; i<num_words; i++){
            bf->insert( words[i] );
        }


        //check
        for(int i=0; i<num_words; i++){
            CPPUNIT_ASSERT_EQUAL (1, (int) bf->contains(words[i]));
        }

        //remove
        for(int i=0; i<num_words; i++){
            bf->remove( words[i] );
        }

        //check
        for(int i=0; i<num_words; i++){
            CPPUNIT_ASSERT_EQUAL (0, (int) bf->contains(words[i]));
        }

        delete bf;
    };




};

// Registers the fixture into the 'registry'
CPPUNIT_TEST_SUITE_REGISTRATION (BloomTests);

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
