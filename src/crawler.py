import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from src.url_frontier import URLFrontier
from src.utils.robots_parser import RobotsParser

class Crawler:
    def __init__(self, start_url, max_urls=10):
        self.start_url = start_url
        self.max_urls = max_urls
        self.url_frontier = URLFrontier()
        self.url_frontier.add_url(start_url)
        self.robots_parser = RobotsParser()
        self.domain = urlparse(start_url).netloc

    def download_url(self, url):
        try:
            response = requests.get(url, timeout=5)
            return response.text
        except Exception as e:
            print(f'Error downloading {url}: {e}')
            return ''

    def get_linked_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def add_url_to_frontier(self, url):
        if urlparse(url).netloc == self.domain:
            self.url_frontier.add_url(url)

    def crawl(self, url):
        if not self.robots_parser.can_fetch(url):
            print(f'Robots.txt disallows crawling {url}')
            return

        html = self.download_url(url)
        for url in self.get_linked_urls(url, html):
            self.add_url_to_frontier(url)

    def run(self):
        while not self.url_frontier.is_empty() and self.url_frontier.crawled_count < self.max_urls:
            url = self.url_frontier.get_next_url()
            print(f'Crawling: {url}')
            try:
                self.crawl(url)
            except Exception as e:
                print(f'Failed to crawl {url}: {e}')
            finally:
                self.url_frontier.mark_as_crawled(url)
            time.sleep(1)  # Be polite, wait a second between requests

if __name__ == '__main__':
    crawler = Crawler('https://example.com', max_urls=10)  # Replace with your target website
    crawler.run()