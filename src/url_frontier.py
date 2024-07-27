from collections import deque

class URLFrontier:
    def __init__(self):
        self.urls_to_crawl = deque()
        self.crawled_urls = set()

    def add_url(self, url):
        if url not in self.crawled_urls and url not in self.urls_to_crawl:
            self.urls_to_crawl.append(url)

    def get_next_url(self):
        return self.urls_to_crawl.popleft() if self.urls_to_crawl else None

    def mark_as_crawled(self, url):
        self.crawled_urls.add(url)

    def is_empty(self):
        return len(self.urls_to_crawl) == 0

    @property
    def crawled_count(self):
        return len(self.crawled_urls)