from django.conf.urls.defaults import *

urlpatterns = patterns('',
    # Example:
    (r'^$', 'py.search.views.search'),    
    (r'^search/', 'py.search.views.search'),
    # Uncomment this for admin:
    # (r'^admin/', include('django.contrib.admin.urls')),
    # catch all
    (r'^.*$', 'py.search.views.search'),   
)
