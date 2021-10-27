# -*- coding: utf-8 -*-
# @Time    : 2021/10/16 14:29
# @Author  : Mike
# @File    : WebOfScience
import re
import time
import scrapy
from bs4 import BeautifulSoup
from scrapy.http import FormRequest
import logging
from urllib.parse import unquote
from ScholarDataset.items import ScholardatasetItem

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


class WebOfScienceSpider(scrapy.Spider):
    name = 'WebOfScience'
    allowed_domains = ['www.webofknowledge.com']
    start_urls = ['https://www.webofknowledge.com/']
    timestamp = str(time.strftime('%Y-%m-%d-%H.%M.%S', time.localtime(time.time())))
    end_year = time.strftime('%Y')

    # 提取URL中的SID和QID所需要的正则表达式
    sid_pattern = r'SID=(\w+)'
    qid_pattern = r'qid=(\d+)'

    # 提取已购买数据库的正则表达式
    # db_pattern = r'WOS\.(\w+)'
    db_list = ['SCI', 'SSCI', 'AHCI', 'ISTP', 'ESCI', 'CCR', 'IC']
    sort_by = "RS.D;PY.D;AU.A;SO.A;VL.D;PG.A"  # 排序方式，相关性第一

    def __init__(self, *args, **kwargs):
        """
        Web Of Science爬虫
        :param kwargs:
            {query_list}: 保存所有查询式的文件的字典，要求列表内每个元素的键为paper_id，值为论文的题目paper_title
        """
        super().__init__(*args, **kwargs)
        self.query_list = kwargs['query_list']
        self.sid = None
        self.qid_list = []

        handler = logging.FileHandler('wos_crawler_log.txt', encoding='utf-8')
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def parse(self, response, **kwargs):
        pattern = re.compile(self.sid_pattern)
        result = re.search(pattern, unquote(response.url))
        if result:
            self.sid = result.group(1)
        else:
            logger.critical('SID提取失败，请检查ip地址是否具有访问权限')
            exit(-1)

        # 提交post高级搜索请求
        adv_search_url = 'https://apps.webofknowledge.com/WOS_AdvancedSearch.do'
        for paper_id, paper_title in self.query_list.items():
            query_form = {
                "product": "WOS",
                "search_mode": "AdvancedSearch",
                "SID": self.sid,
                "input_invalid_notice": "Search Error: Please enter a search term.",
                "input_invalid_notice_limits": " <br/>Note: Fields displayed in scrolling boxes must be combined with at least one other search field.",
                "action": "search",
                "replaceSetId": "",
                "goToPageLoc": "SearchHistoryTableBanner",
                "value(input1)": 'TI=(' + paper_title + ')',
                "value(searchOp)": "search",
                "value(select2)": "LA",
                "value(input2)": "",
                "value(select3)": "DT",
                "value(input3)": "",
                "value(limitCount)": "14",
                "limitStatus": "expanded",
                "ss_lemmatization": "On",
                "ss_spellchecking": "Suggest",
                "SinceLastVisit_UTC": "",
                "SinceLastVisit_DATE": "",
                "period": "Range Selection",
                "range": "ALL",
                "startYear": "1900",
                "endYear": self.end_year,
                "editions": self.db_list,
                "update_back2search_link_param": "yes",
                "ss_query_language": "",
                "rs_sort_by": self.sort_by,
            }

            yield FormRequest(adv_search_url, method='POST', formdata=query_form, dont_filter=True,
                              callback=self.parse_query_response,
                              meta={'sid': self.sid, 'query': paper_title, 'paper_id': paper_id})

    def parse_query_response(self, response):
        sid = response.meta['sid']
        query = response.meta['query']

        # 通过bs4解析html找到检索结果的入口
        soup = BeautifulSoup(response.text, 'lxml')
        entry = soup.find('a', attrs={'title': 'Click to view the results'})

        if not entry:
            logger.warning(f"对于'{query}'，未找到任何内容")
            return
        entry_url = 'https://apps.webofknowledge.com' + entry.get('href')

        # 找到入口url中的QID，存放起来以供下一步处理函数使用
        pattern = re.compile(self.qid_pattern)
        result = re.search(pattern, entry_url)
        if result:
            qid = result.group(1)
            if qid in self.qid_list:
                logger.warning(f"发现重复爬取现象，可能是因为'{query}'未找到任何内容")
                return
            self.qid_list.append(qid)
        else:
            logger.error(f'对于"{query}"，qid提取失败')
            exit(-1)

        # 爬第一篇
        start = 1
        end = 1
        paper_num = 1

        output_form = {
            "selectedIds": "",
            "displayCitedRefs": "true",
            "displayTimesCited": "true",
            "displayUsageInfo": "true",
            "viewType": "summary",
            "product": "WOS",
            "rurl": response.url,
            "mark_id": "WOS",
            "colName": "WOS",
            "search_mode": "AdvancedSearch",
            "locale": "en_US",
            "view_name": "WOS-summary",
            "sortBy": self.sort_by,
            "mode": "OpenOutputService",
            "qid": str(qid),
            "SID": str(sid),
            "format": "saveToExcel",  # txt: saveToFile
            "filters": "HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND AUTHORSIDENTIFIERS ACCESSION_NUM FUNDING SUBJECT_CATEGORY JCR_CATEGORY LANG IDS PAGEC SABBR CITREFC ISSN PUBINFO KEYWORDS CITTIMES ADDRS CONFERENCE_SPONSORS DOCTYPE CITREF ABSTRACT CONFERENCE_INFO SOURCE TITLE AUTHORS  ",
            "mark_to": str(end),
            "mark_from": str(start),
            "queryNatural": str(query),
            "count_new_items_marked": "0",
            "use_two_ets": "false",
            "IncitesEntitled": "yes",
            "value(record_select_type)": "range",
            "markFrom": str(start),
            "markTo": str(end),
            "fields_selection": "HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND AUTHORSIDENTIFIERS ACCESSION_NUM FUNDING SUBJECT_CATEGORY JCR_CATEGORY LANG IDS PAGEC SABBR CITREFC ISSN PUBINFO KEYWORDS CITTIMES ADDRS CONFERENCE_SPONSORS DOCTYPE CITREF ABSTRACT CONFERENCE_INFO SOURCE TITLE AUTHORS  ",
            # "save_options": "xls"
        }

        output_url = 'https://apps.webofknowledge.com/OutboundService.do?action=go&&save_options=xls'
        yield FormRequest(output_url, method='POST', formdata=output_form, dont_filter=True,
                          callback=self.item_download,
                          meta={'query': query, 'paper_id': response.meta['paper_id']})

    def item_download(self, response):
        item = ScholardatasetItem()
        item['content'] = response.body
        item['query'] = response.meta['query']
        item['paper_id'] = response.meta['paper_id']
        yield item
