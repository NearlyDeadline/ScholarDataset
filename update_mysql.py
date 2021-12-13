# -*- coding: utf-8 -*-
# @Time    : 2021/10/16 20:09
# @Author  : Mike
# @File    : update_mysql
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
handler = logging.FileHandler("update_log.txt", encoding='utf-8')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


@defer.inlineCallbacks
def update_mysql(args):
    crawler_paper_count = args.crawler_paper_count
    crawler_name = args.crawler_name
    connection_config = json.load(open('./ScholarDataset/config.json'))
    with pymysql.connect(host=connection_config['host'],
                         user=connection_config['user'],
                         password=connection_config['password'],
                         database=connection_config['database']) as connection:
        with connection.cursor() as cursor:
            # wos爬虫只爬期刊，其他爬虫都爬
            sql = \
                "SELECT id, title FROM paper WHERE venue in (SELECT name FROM venue WHERE kind='journal') " \
                "and id IN (SELECT pid FROM author_paper WHERE aid in " \
                "(SELECT id FROM author WHERE email='' AND university='' AND college='' AND lab=''));" \
                    if crawler_name == 'WebOfScience' else \
                    "SELECT id, title FROM paper WHERE id IN (SELECT pid FROM author_paper " \
                    "WHERE aid in (SELECT id FROM author WHERE email='' AND university='' AND college='' AND lab=''));"
            cursor.execute(sql)

            result = cursor.fetchmany(crawler_paper_count)
            while result:
                query_list = {}
                for i in result:
                    paper_id = i[0]
                    paper_title = i[1]
                    query_list[paper_id] = paper_title
                try:
                    yield runner.crawl(crawler_name, query_list=query_list)
                except SystemExit:
                    logger.error(f'发生了{crawler_name}爬虫错误，请检查该文件夹内爬虫日志文件')
                result = cursor.fetchmany(crawler_paper_count)

    reactor.stop()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--paper_count', help='爬虫每次爬取的论文数', dest='crawler_paper_count', type=int, default=150)
    ap.add_argument('--crawler_name', help='爬虫名称，只能为WebOfScience/ACM/IEEExplore', dest='crawler_name', type=str, required=True,
                    choices=['WebOfScience', 'ACM', 'IEEExplore'])
    args = ap.parse_args()
    update_mysql(args=args)
    reactor.run()
