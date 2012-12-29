__author__ = 'prince'

import requests
import grequests
import BeautifulSoup
import Queue
import urlparse
import threading
from sqlobject import *
import sqlite3 as sqlite
import time
from requests import async

def _get_html_from_response_obj(response_obj):
    """
    fetches the resource pointed to by the url.

    NOT ALL URLs ARE MEANT TO BE CRAWLED OVER

    Case 1:
    if http response status_code is not 200, its NOT something we are interested in

    Case 2:
    By default, when you make a request, the body of the response is not downloaded immediately.
    So parse headers first, and download content later

    Case 3:
    If its an HTTPS request, we need to verify SSL certificates

    We need to get the RESPONSE HEADERS first and content LATER
    """
    html = ''
    valid_content_types = ['text/html']
    try:
        valid_status_code = response_obj.status_code == 200
        content_type = response_obj.headers.get('content-type', False)
        valid_content_type = content_type.startswith(valid_content_types[0])
        if  valid_status_code and valid_content_type :
            #wow. valid response
            html        = response_obj.content
    except Exception, fault:
        print "Error occured while fetching html"
        print str(fault)
    return html

def _get_anchor_tags_from_html(html):
    parsed_html = BeautifulSoup.BeautifulSoup(html)
    anchor_tags = parsed_html.findAll('a')
    return anchor_tags

def _get_valid_links_from_anchor_list(a_list = [], base_url=''):
    """
    we are crazily thirsty for valid urls.
    so if we hit a url like 'http://example.com/a/b/sometext.html',
    we will also crawl 'http://example.com/a' and 'http://example.com/a/b'
    """
    valid_links = []
    for item in a_list:
        href = item.get('href')
        if href:
            valid_links += _get_valid_links_list(base_url, href)
    return valid_links

def _check_duplicate(repo, item):
    """
    check if item exists in repo
    """
    pass

def _get_valid_links_list(url, href):
    """
    scheme should be in [http, https]
    Links can be:
    1. full urls (e.g. http://google.com), or
    2. reference to local items (e.g. /docs) which should translate to http://python.org/docs, or
    3. permalinks to items on same page e.g. #downloads. I will leave this part though.
    """
    valid_schemes   = ['http', 'https']
    url             = url.strip('/')
    href            = href.strip('/')
    parsed_href     = urlparse.urlsplit(href)
    skip_base       = False
    parsed_base_url = urlparse.urlsplit(url)
    temp_url        = ''
    valid_urls      = []
    if parsed_href.scheme:
        temp_url = href if (parsed_href.scheme in valid_schemes) else ''
        if parsed_href.netloc == parsed_base_url.netloc:
            skip_base = True # no need to include base url in the results. its already in the repo
    else:
        if parsed_href.path or parsed_href.query:
            temp_url = urlparse.urljoin(url, href)
            skip_base = True
    # at this point, we have something like 'http://python.org/assets/docs/about.html#somediv'
    # temp_url is one valid url.
    # now lets get some more
    if temp_url:
        valid_urls = _find_isolated_resources(temp_url, skip_base) if temp_url else []
    return valid_urls

def _find_isolated_resources(url, skip_base = False):
    """
    Returns all ascending urls from passed url.
    e.g. http://python.org/foo/bar/baz should return
    ['http://python.org', 'http;//python.org/foo', 'http://python.org/foo/bar', 'http;//python.org/foo/bar/baz']
    """
    urls = []
    parsed_url   = urlparse.urlsplit(url)
    temp_paths   = parsed_url.path.strip('/').split('/')
    # forget about the query and fragment
    base_url    = urlparse.urlunsplit(urlparse.SplitResult(parsed_url.scheme, parsed_url.netloc, '', '', ''))
    if not skip_base:
        urls.append(base_url)
    for path in temp_paths:
        new_url = _path_join(base_url, path) # there is a reason why I didn't use urlparse.urljoin
        urls.append(new_url)
        base_url = _path_join(base_url, path)
    return urls

def _path_join(base, edge):
    """
     adds edge to base.
     i.g. it returns foo/bar for (foo, bar) and foo for (foo, '')
    """
    return base + '/' + edge if edge else base

def connect_db(db_path):
    connection_string = 'sqlite:' + db_path
    connection = connectionForURI(connection_string)
    sqlhub.processConnection = connection

def init_db():
    Url.createTable(ifNotExists=True)

class Url(SQLObject):
    url      = UnicodeCol(length = 1024, unique = True) # lets ensure unique links at db level as well
    visited = BoolCol(default = False)

if __name__=="__main__":
    connect_db('urls.db')
    init_db()
    cur_count = Url.select().count()
    max_count = 800
    while cur_count < max_count:
        if cur_count == 0:
            # new db. seed url
            Url(url = 'http://python.org')
        query = Url.select()
        query = query.filter(Url.q.visited == False)
        count = query.count()
        if not count:
            break;
        urls = []
        for u in list(query.limit(5)):
            urls.append(u.url)
            u.visited = True
        print urls
        rs = (grequests.get(u) for u in urls)
        i = 0
        links = []
        for item in grequests.map(rs, False, 5):
            html = _get_html_from_response_obj(item)
            anchor_tags = _get_anchor_tags_from_html(html)
            valid_links = _get_valid_links_from_anchor_list(anchor_tags, urls[i])
            links += valid_links
            i += 1
        for l in links:
            try:
                if not cur_count < max_count:
                    break
                nurl = Url(url = l)
            except Exception, fault:
                print str(fault)
            else:
                cur_count += 1
                print "current count ", cur_count

    print "Exiting"

