# -*- coding: utf-8 -*-
# @Time    : 2021/11/10 15:07
# @Author  : 12897
# @File    : IEEExplore.py
from urllib.parse import quote
import scrapy
from scrapy import FormRequest, Request
from ScholarDataset.items import ScholardatasetItem
import json
import re
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


class IEEExploreSpider(scrapy.Spider):
    name = 'IEEExplore'
    allowed_domains = ['ieeexplore.ieee.org']
    start_urls = ['https://ieeexplore.ieee.org/']

    pattern = 'xplGlobal.document.metadata=\{.*\};'

    def __init__(self, *args, **kwargs):
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
            yield FormRequest(search_url, method='POST', formdata=json.dumps(query_form), dont_filter=True,
                              callback=self.parse_query_response, headers=headers,
                              meta={'query': paper_title, 'paper_id': paper_id})

    def parse_query_response(self, response):
        query = response.meta['query']
        result = json.loads(response.text)
        if result.get('records') is None:
            logger.warning(f"对于'{query}'，未在IEEExplore网站上找到任何内容")
            return
        entry_url = f"https://ieeexplore.ieee.org{result['records'][0]['htmlLink']}"  # 只选择第一篇论文
        yield Request(url=entry_url, callback=self.item_download, dont_filter=True,
                      meta={'query': response.meta['query'], 'paper_id': response.meta['paper_id']}
                      )

    def item_download(self, response):
        item = ScholardatasetItem()
        content = re.search(self.pattern, response.text)
        item['content'] = json.loads(content[len('xplGlobal.document.metadata='): -1])  # 最后有个分号，去掉。前面这些变量名也去掉，形成字典
        item['query'] = response.meta['query']
        item['paper_id'] = response.meta['paper_id']
        yield item
