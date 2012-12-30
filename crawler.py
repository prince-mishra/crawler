__author__ = 'prince'

import requests
from sqlobject import *
import sqlite3 as sqlite
import time

from LinkFetcher import LinkFetcher

class Url(SQLObject):
    url     = UnicodeCol(length = 1024, unique = True) # lets ensure unique links at db level
    visited = BoolCol(default = False)

class Crawler:
    """
    Web crawling for superhumans! this is superfast, super greedy!
    """
    def __init__(self, db_path, max_urls, seed_url = 'http://python.org'):
        self.db_path        = db_path
        self.max_urls       = max_urls
        self.seed_url       = seed_url
        self.max_parallel_connections = 5
        self.link_fetcher   = LinkFetcher()
        self.connect_db()
        self.init_db()
        if self.uncrawled_links_count() == 0:
            self.seed_db()

    def start_crawling(self):
        while not self.stop_condition():
            urls            = self.get_uncrawled_urls(self.max_parallel_connections)
            rdict           = self.link_fetcher.fetch(urls)
            parsed_links    = self.link_fetcher.parse(rdict)
            self.insert_to_db(parsed_links)

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

if __name__=="__main__":
    t1 = time.time()
    c = Crawler('urls2.db', 1000)
    c.start_crawling()
    t2 = time.time()
    print "Time taken : ", int(t2 - t1), "seconds"
    print "Exiting"

