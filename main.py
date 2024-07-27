from src.crawler import Crawler

def main():
    start_url = 'https://example.com'  # Replace with your target website
    max_urls = 10
    crawler = Crawler(start_url, max_urls)
    crawler.run()

if __name__ == '__main__':
    main()