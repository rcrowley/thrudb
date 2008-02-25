#ifdef HAVE_CONFIG_H
#include "thrucommon_config.h"
#endif
/* hack to work around thrift and log4cxx installing config.h's */
#undef HAVE_CONFIG_H

#include <stdio.h>

#if HAVE_CPPUNIT

#include "SpreadConnection.h"

#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <log4cxx/propertyconfigurator.h>
#include <string>

using namespace std;
using namespace log4cxx;

class SpreadTest : public CppUnit::TestFixture
{
    public:
        CPPUNIT_TEST_SUITE (SpreadTest);
        CPPUNIT_TEST (testAll);
        CPPUNIT_TEST_SUITE_END ();

        void testAll ()
        {
            string spread_name = "4803@localhost";
            SpreadConnection * conn = 
                new SpreadConnection (spread_name, "cppunit");
            CPPUNIT_ASSERT (conn); 
            
            // TODO: make this randomish
            string group = "testing";

            // start up srpead
            all_callback_count = 0;
            SubscriberCallbackInfo all_info;
            all_info.callback = &SpreadTest::all_callback;
            all_info.data = this;
            conn->subscribe ("", "", -1, &all_info);

            sender_callback_count = 0;
            SubscriberCallbackInfo sender_info;
            sender_info.callback = &SpreadTest::sender_callback;
            sender_info.data = this;
            conn->subscribe (conn->get_private_group (), "", -1, &sender_info);

            group_callback_count = 0;
            SubscriberCallbackInfo group_info;
            group_info.callback = &SpreadTest::group_callback;
            group_info.data = this;
            conn->subscribe ("", group, -1, &group_info);

            type_callback_count = 0;
            SubscriberCallbackInfo type_info;
            type_info.callback = &SpreadTest::type_callback;
            type_info.data = this;
            conn->subscribe ("", "", 42, &type_info);

            once_callback_count = 0;
            SubscriberCallbackInfo once_info;
            once_info.callback = &SpreadTest::once_callback;
            once_info.data = this;
            conn->subscribe ("", "", -1, &once_info);

            const char msg[] = "hello world!";
            int msg_len = strlen (msg);

            // TODO: tmp clean out the join message
            // receive stuff we send out
            conn->set_receive_self (true);

            // everything
            conn->queue (group, 42, msg, strlen (msg));
            conn->run (1);
            CPPUNIT_ASSERT_EQUAL (1, this->all_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->sender_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->group_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->type_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->once_callback_count);

            // other type, once removed
            conn->queue (group, 43, msg, msg_len);
            conn->run (1);
            CPPUNIT_ASSERT_EQUAL (2, this->all_callback_count);
            CPPUNIT_ASSERT_EQUAL (2, this->sender_callback_count);
            CPPUNIT_ASSERT_EQUAL (2, this->group_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->type_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->once_callback_count);

            // other group
            conn->queue (group, 42, msg, msg_len);
            conn->run (1);
            CPPUNIT_ASSERT_EQUAL (3, this->all_callback_count);
            CPPUNIT_ASSERT_EQUAL (3, this->sender_callback_count);
            CPPUNIT_ASSERT_EQUAL (2, this->group_callback_count);
            CPPUNIT_ASSERT_EQUAL (2, this->type_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->once_callback_count);

            // other sender...
            SpreadConnection * other_conn = 
                new SpreadConnection (spread_name, "other_cppunit");
            other_conn->queue (group, 42, msg, msg_len);
            other_conn->run (-1);
            conn->run (1);
            CPPUNIT_ASSERT_EQUAL (4, this->all_callback_count);
            CPPUNIT_ASSERT_EQUAL (3, this->sender_callback_count);
            CPPUNIT_ASSERT_EQUAL (3, this->group_callback_count);
            CPPUNIT_ASSERT_EQUAL (3, this->type_callback_count);
            CPPUNIT_ASSERT_EQUAL (1, this->once_callback_count);

            delete other_conn;
            delete conn;

            // shut down spread
        };

        static bool all_callback (SpreadConnection * /* spread_connection */,
                                  const std::string & /* sender */,
                                  const std::string & /* group */,
                                  const int /* message_type */,
                                  const char * /* message */,
                                  const int /* message_len */,
                                  void * data)
        {
            ((SpreadTest*)data)->all_callback_count++;
            return 1;
        }

        static bool sender_callback (SpreadConnection * /* spread_connection */,
                                     const std::string & /* sender */,
                                     const std::string & /* group */,
                                     const int /* message_type */,
                                     const char * /* message */,
                                     const int /* message_len */,
                                     void * data)
        {
            ((SpreadTest*)data)->sender_callback_count++;
            return 1;
        }

        static bool group_callback (SpreadConnection * /* spread_connection */,
                                    const std::string & /* sender */,
                                    const std::string & /* group */,
                                    const int /* message_type */,
                                    const char * /* message */,
                                    const int /* message_len */,
                                    void * data)
        {
            ((SpreadTest*)data)->group_callback_count++;
            return 1;
        }

        static bool type_callback (SpreadConnection * /* spread_connection */,
                                   const std::string & /* sender */,
                                   const std::string & /* group */,
                                   const int /* message_type */,
                                   const char * /* message */,
                                   const int /* message_len */,
                                   void * data)
        {
            ((SpreadTest*)data)->type_callback_count++;
            return 1;
        }

        static bool once_callback (SpreadConnection * /* spread_connection */,
                                   const std::string & /* sender */,
                                   const std::string & /* group */,
                                   const int /* message_type */,
                                   const char * /* message */,
                                   const int /* message_len */,
                                   void * data)
        {
            ((SpreadTest*)data)->once_callback_count++;
            return 0;
        }

    private:
        int all_callback_count;
        int sender_callback_count;
        int group_callback_count;
        int type_callback_count;
        int once_callback_count;

};

// Registers the fixture into the 'registry'
CPPUNIT_TEST_SUITE_REGISTRATION (SpreadTest);

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
    return 1;
}

#endif /* HAVE_CPPUNIT */
