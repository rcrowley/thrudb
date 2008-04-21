from django.conf.urls.defaults import *

urlpatterns = patterns('',
    # Example:
    (r'^$', 'tweetsearch.search.views.search'),    
    (r'^search/', 'tweetsearch.search.views.search'),
    # Uncomment this for admin:
    # (r'^admin/', include('django.contrib.admin.urls')),
    # catch all
    (r'^.*$', 'tweetsearch.search.views.search')  
)
