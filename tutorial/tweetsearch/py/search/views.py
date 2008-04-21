# Create your views here.
from django.shortcuts import render_to_response
from twitter import TweetManager
from time import time

tm = TweetManager()

def search(request):
    query = request.GET.get('q', '').encode('utf-8')
    offset = request.GET.get('offset', 0)
    if offset:
        offset = int(offset)  
    tweets = None
    total = None
    next = None
    prev = None
    took = None
    if query:
        t0 = time()
        (total, tweets) = tm.search_tweet(query, offset)
        took = "%.2f" % (time() - t0)
        next = (total > len(tweets) + offset) and (offset + len(tweets)) or None
        prev = (offset > 0) and (offset - 10) or None

    return render_to_response("search/template.html", {
        "tweets": tweets,
        "query": query,
        "took" : took,
        "total" : total,
        "current": offset,
        "next": next,
        "prev": prev
       })      
