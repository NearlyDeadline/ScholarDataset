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
def update_wos(args_):
    author_id_list = args_.author_id_list
    connection_config = json.load(open('./ScholarDataset/config.json'))
    query_list = []
    with pymysql.connect(host=connection_config['host'],
                         user=connection_config['user'],
                         password=connection_config['password'],
                         database=connection_config['database']) as connection:
        with connection.cursor() as cursor:
            for aid in author_id_list:
                sql = f"SELECT title FROM paper WHERE id in (SELECT pid FROM author_paper WHERE aid={aid});"
                paper_count = cursor.execute(sql)
                for i in range(0, paper_count):
                    paper_title = cursor.fetchone()[0]
                    logger.info(f'对于作者aid={aid}，查询到题目为{paper_title}')
                    query_list.append(paper_title)
            try:
                yield runner.crawl('WebOfScience', query_list=query_list)
            except SystemExit:
                logger.error(f'发生了Web of Science爬虫错误，请检查该文件夹内爬虫日志文件')
            reactor.stop()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(usage='根据Author ID在Web of science网站上更新该作者所有论文的信息\n参数：\n  --aid')
    ap.add_argument('--author_id_list', help='Author IDs', dest='author_id_list', action="extend", nargs='+', type=int,
                    required=True)
    args = ap.parse_args(
        ['--author_id_list', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '13', '15', '16', '17', '18', '19', '20', '23', '25', '26', '29', '30', '32', '33', '35'])
    update_wos(args)
    reactor.run()
