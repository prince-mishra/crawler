PyCrawler
=========
A crawler is a program that starts with a url on the web (ex: http://python.org), fetches the web-page corresponding to that url, and parses all the links on that page into a repository of links. Next, it fetches the contents of any of the url from the repository just created, parses the links from this new content into the repository and continues this process for all links in the repository until stopped or after a given number of links are fetched.

What's new here
===============

Faster - has URL Throttling (async url fetching)
------------------------------------------------
Like everyone else, our PyCrawler is crazy about speed. What it cannot stand is the loooong wait time when one url is being fetched and others are in queue. If there is network bandwidth available, processing power available, then why wait? 
So it uses grequests (https://github.com/kennethreitz/grequests) to fetch urls asyncronously. By default, 5 urls are fetched concurrently. Yes, 5 is 5 times 1 - as achieved by urllib.urlopen() or requests.get()

Greedier - gets all possible urls from a page
---------------------------------------------
Well, PyCrawler is a real greedy creature!
If a page has a link to 'http://foo.com/bar/baz', not only will it get 'http://foo.com/bar/baz', but it will also get 'http://foo.com' and 'http://foo.com/bar' as well, which, otherwise, would have been ignored. Cool! Ain't it?  


