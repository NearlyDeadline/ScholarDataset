# -*- coding: utf-8 -*-
# @Time    : 2021/11/10 13:09
# @Author  : 12897
# @File    : ACM.py
import scrapy
from bs4 import BeautifulSoup
import requests
from ScholarDataset.items import ScholardatasetItem
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
logger.propagate = False


class ACMDigitalLibrarySpider(scrapy.Spider):
    name = 'ACM'
    allowed_domains = ['dl.acm.org']
    start_urls = ['https://dl.acm.org/']
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://dl.acm.org/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36 Edg/95.0.1020.40'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_list = kwargs['query_list']

        handler = logging.FileHandler('acm_crawler_log.txt', encoding='utf-8')
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def start_requests(self):
        search_url = 'https://dl.acm.org/action/doSearch?AllField='

        for paper_id, paper_title in self.query_list.items():
            search_response = requests.get(url=search_url + paper_title.replace(' ', '+'), headers=self.headers)
            soup = BeautifulSoup(search_response.text, 'lxml')
            entry = soup.find('li', class_='search__item issue-item-container')
            if entry is None:
                logger.warning(f"对于'{paper_title}'，未在ACM网站上找到任何内容")
                return
            entry_url = 'https://dl.acm.org' + entry.find('a').get('href')

            item = ScholardatasetItem()
            item['content'] = (requests.get(url=entry_url)).text
            item['query'] = paper_title
            item['paper_id'] = paper_id
            yield item

    def parse(self, response, **kwargs):
        pass
