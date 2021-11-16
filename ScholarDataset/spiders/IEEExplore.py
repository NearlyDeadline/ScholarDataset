# -*- coding: utf-8 -*-
# @Time    : 2021/11/10 15:07
# @Author  : 12897
# @File    : IEEExplore.py
from urllib.parse import quote
import scrapy
from ScholarDataset.items import ScholardatasetItem
import json
import re
import logging
import requests

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
logger.propagate = False


class IEEExploreSpider(scrapy.Spider):
    name = 'IEEExplore'
    allowed_domains = ['ieeexplore.ieee.org']
    start_urls = ['https://ieeexplore.ieee.org/']

    pattern = 'xplGlobal.document.metadata=\{.*\};'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_list = kwargs['query_list']

        handler = logging.FileHandler('ieee_crawler_log.txt', encoding='utf-8')
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def parse(self, response, **kwargs):
        search_url = 'https://ieeexplore.ieee.org/rest/search'
        for paper_id, paper_title in self.query_list.items():
            headers = {
                'Accept': 'application/json,text/plain,*/*',
                'Accept-Encoding': 'gzip,deflate,br',
                'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Length': '122',
                'Content-Type': 'application/json',
                'Referer': f'https://ieeexplore.ieee.org/search/searchresult.jsp?newsearch=true&queryText={quote(paper_title)}',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0'

            }
            query_form = {
                'newsearch': 'true',
                'queryText': paper_title,
            }

            search_response = requests.post(url=search_url, data=json.dumps(query_form), headers=headers)
            search_result = json.loads(search_response.text)
            papers = search_result.get('records')
            if papers is None:
                logger.warning(f"对于'{paper_title}'，未在IEEExplore网站上找到任何内容")
                return
            html_link = papers[0]['htmlLink']
            document_url = f'https://ieeexplore.ieee.org{html_link}'
            document_response = requests.get(url=document_url)
            data = re.search(self.pattern, document_response.text)
            s = data.group()
            content = json.loads(s[len('xplGlobal.document.metadata='): -1])
            item = ScholardatasetItem()
            item['content'] = content
            item['query'] = paper_title
            item['paper_id'] = paper_id
            yield item
