__author__ = 'prince'

import grequests
import BeautifulSoup
import urlparse

class LinkFetcher:
    """
    fetches contents of a url, parses the html content and
    generates a list of valid urls
    """
    def __init__(self, prefetch = False, max_parallel_connections = 5,
                 valid_content_types = ['text/html'], verify_ssl = True):
        self.prefetch = prefetch
        self.max_parallel_connections = max_parallel_connections
        self.valid_content_types = valid_content_types
        self.verify_ssl = verify_ssl

    def fetch(self, urls):
        """
        uses grequests to make requests parallely
        returns a dict of the form {'url' : 'response'}

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
        rdict = {}
        try:
            rs = (grequests.get(u) for u in urls) # TODO: add verify ssl support
            response_objs = grequests.map(rs, self.prefetch, self.max_parallel_connections) #Parallel Fetch
            rdict = dict(zip(urls, response_objs))
        except Exception, fault:
            # handle exception
            print "Error in fetching ", str(fault)
        return rdict

    def parse(self, rdict):
        print "parsing"
        links = []
        for url in rdict.keys():
            html    = self.get_html(rdict[url])
            if not html:
                continue
            a_tags  = self.get_a_tags_from_html(html)
            print "got a tags", len(a_tags)
            _links  = []
            for a in a_tags:
                href = a.get('href')
                if href:
                    _links += self.extract_links_from_a_href(url, href)
            links += _links
        return links

    def get_html(self, response_obj):
        html = ''
        try:
            is_valid_status_code = response_obj.status_code == 200
            content_type = response_obj.headers.get('content-type', False)
            is_valid_content_type = False
            if content_type:
                for c in self.valid_content_types:
                    if content_type.startswith(c):
                        is_valid_content_type = True
            if  is_valid_content_type and is_valid_status_code:
                #wow. valid response
                html        = response_obj.content
        except Exception, fault:
            print "Error occured while fetching html"
            print str(fault)
        return html

    def get_a_tags_from_html(self, html):
        parsed_html = BeautifulSoup.BeautifulSoup(html)
        anchor_tags = parsed_html.findAll('a')
        return anchor_tags

    def extract_links_from_a_href(self, url, href):
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
            valid_urls = self._find_isolated_resources(temp_url, skip_base) if temp_url else []
        return valid_urls

    def _find_isolated_resources(self, url, skip_base = False):
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
            new_url = self._path_join(base_url, path) # there is a reason why I didn't use urlparse.urljoin
            urls.append(new_url)
            base_url = self._path_join(base_url, path)
        return urls

    def _path_join(self, base, edge):
        """
         adds edge to base.
         i.g. it returns foo/bar for (foo, bar) and foo for (foo, '')
        """
        return base + '/' + edge if edge else base

if __name__ == "__main__":
    urls = ['http://python.org', 'https://google.co.in']
    l = LinkFetcher()
    r = l.fetch(urls)
    print r
    p = l.parse(r)
    print p