__author__ = 'prince'

import requests
import grequests # Requires gevent
import BeautifulSoup
import urlparse
from sqlobject import *
import sqlite3 as sqlite
import time

class Url(SQLObject):
    url      = UnicodeCol(length = 1024, unique = True) # lets ensure unique links at db level
    visited = BoolCol(default = False)

class Crawler:
    """
    Web crawling for superhumans! this is superfast, super greedy!
    """
    def __init__(self, db_path, max_urls, seed_url = 'http://python.org'):
        self.db_path    = db_path
        self.max_urls   = max_urls
        self.seed_url   = seed_url
        self.max_parallel_connections = 5
        self.connect_db()
        self.init_db()
        if self.uncrawled_links_count() == 0:
            self.seed_db()

    def start_crawling(self):
        while not self.stop_condition():
            urls            = self.get_uncrawled_urls(self.max_parallel_connections)
            parsed_links    = self.fetch_and_parse(urls)
            self.insert_to_db(parsed_links)

    def fetch_and_parse(self, urls):
        rs = (grequests.get(u) for u in urls)
        i = 0
        links = []
        _PREFETCH = False
        _MAX_PARALLEL_CONNECTIONS = self.max_parallel_connections
        for item in grequests.map(rs, _PREFETCH, _MAX_PARALLEL_CONNECTIONS): #Parallel Fetch
            html = _get_html_from_response_obj(item)
            anchor_tags = _get_anchor_tags_from_html(html)
            valid_links = _get_valid_links_from_anchor_list(anchor_tags, urls[i])
            links += valid_links
            i += 1
        return links

    def insert_to_db(self, links):
        # remove duplicates, if any!
        s = set(links)
        unique_links = list(s)
        total = len(unique_links)
        success = 0
        failed = 0
        for item in unique_links:
            if self.stop_condition():
                break
            try:
                print "current strength ", self.total_links_count()
                Url(url = item)
            except Exception, fault:
                print "insert failed. Error ", str(fault) # printing item here is not unicode safe!
                failed += 1
            else:
                success += 1
        return total, success, failed

    def connect_db(self):
        connection_string = 'sqlite:' + self.db_path
        connection = connectionForURI(connection_string)
        sqlhub.processConnection = connection

    def init_db(self):
        Url.createTable(ifNotExists=True)

    def seed_db(self):
        try:
            Url(url = self.seed_url)
        except Exception, fault:
            print "Cannot create seed url"
            raise

    def uncrawled_links_count(self):
        query = Url.select(Url.q.visited == False)
        return query.count()

    def total_links_count (self):
        query = Url.select()
        return query.count()

    def get_uncrawled_urls(self, count):
        query = Url.select(Url.q.visited == False).limit(count)
        ret = []
        for u in list(query):
            u.visited = True
            ret.append(u.url)
        return ret

    def stop_condition(self):
        return self.total_links_count() > self.max_urls

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
        content_type_valid = False
        if content_type:
            content_type_valid = content_type.startswith(valid_content_types[0])
        if  valid_status_code and content_type_valid :
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

if __name__=="__main__":
    t1 = time.time()
    c = Crawler('urls2.db', 1000)
    c.start_crawling()
    t2 = time.time()
    print "Time taken : ", int(t2 - t1)
    print "Exiting"

