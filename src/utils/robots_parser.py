import urllib.robotparser
import urllib.parse

class RobotsParser:
    def __init__(self):
        self.parser = urllib.robotparser.RobotFileParser()
        self.robot_cache = {}

    def fetch_robots_txt(self, url):
        if url not in self.robot_cache:
            self.parser.set_url(f'{url}/robots.txt')
            self.parser.read()
            self.robot_cache[url] = self.parser

    def can_fetch(self, url):
        domain = urllib.parse.urlparse(url).netloc
        self.fetch_robots_txt(f'https://{domain}')
        return self.robot_cache[f'https://{domain}'].can_fetch('*', url)