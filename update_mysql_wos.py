# -*- coding: utf-8 -*-
# @Time    : 2021/10/16 20:09
# @Author  : Mike
# @File    : main
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import logging
import pymysql
import json
import argparse

configure_logging()
runner = CrawlerRunner(get_project_settings())

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("main_log.txt", encoding='utf-8')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


@defer.inlineCallbacks
def update_wos(args):
    connection_config = json.load(open('./ScholarDataset/config.json'))
    with pymysql.connect(host=connection_config['host'],
                         user=connection_config['user'],
                         password=connection_config['password'],
                         database=connection_config['database']) as connection:
        with connection.cursor() as cursor:
            sql = f"SELECT id, title FROM paper WHERE venue in (SELECT name FROM venue WHERE kind='journal');"
            cursor.execute(sql)
            wos_crawler_paper_count = 150  # 一次取150篇文章，防止爬虫过载
            result = cursor.fetchmany(wos_crawler_paper_count)
            while result:
                query_list = {}
                for i in result:
                    paper_id = i[0]
                    paper_title = i[1]
                    query_list[paper_id] = paper_title
                try:
                    yield runner.crawl('WebOfScience', query_list=query_list)
                except SystemExit:
                    logger.error(f'发生了Web of Science爬虫错误，请检查该文件夹内爬虫日志文件')
                result = cursor.fetchmany(wos_crawler_paper_count)

    reactor.stop()


if __name__ == '__main__':
    update_wos(args=None)
    reactor.run()
