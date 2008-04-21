HOW TO RUN

1. install django (http://www.djangoproject.com), apache, mod_python, and other necessary components

2. catch the tweet by running ./search/twitter.py, you can see the processs in /tmp/twitter.log

3. config apache with mod_python. Here's a sample conf:

<VirtualHost *>
	ServerName tweetsearch.local
        DocumentRoot /path/to/thrudb/tutorial/tweetsearch/py
        <Location "/">
                SetHandler python-program
                PythonHandler   django.core.handlers.modpython
                SetEnv  DJANGO_SETTINGS_MODULE py.settings
                SetEnv  PYTHON_EGG_CACHE /tmp
                PythonDebug On
                PythonPath      "['/path/to/thrudb/tutorial/tweetsearch/py'] + sys.path"
        </Location>

</VirtualHost>

remember add this line to /etc/hosts:
127.0.0.1 tweetsearch.local

4. start apache, thrudex and thrudoc

5. that's it! Contact me if you have any problem.

Thai Duong (thaidn@gmail.com).





  
