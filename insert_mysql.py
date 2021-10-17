# -*- coding: utf-8 -*-
# @Time    : 2021/10/16 20:08
# @Author  : Mike
# @File    : insert_mysql
import pymysql
from pymysql.converters import escape_string
import os
import glob
import pandas as pd
import logging
import json

#  a = 'xplGlobal.document.metadata=\{.*\};' //提取IEEExplore元信息的正则表达式
# 为sql语句中变量增加escape_string方法调用的正则表达式：
# '\{([\S]*)\}'
# '\{escape_string($1)\}'
data_dirs = [r'E:\PythonProjects\ScholarDataset\data\test\input\计算机']
need_disambiguation_pattern = '*_disambiguation_article.csv'
dont_need_disambiguation_pattern = '*_undisambiguation_article.csv'

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("./log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def insert_csv_into_mysql(csv_file: str, need_disambiguation: bool, author_title: str,
                          mysql_connect):
    def get_venue_kind(kind: str) -> str:
        journal_pattern = '<journal>'
        crossref_pattern = '<crossref>'
        if kind.startswith(journal_pattern):
            return 'journal'
        elif kind.startswith(crossref_pattern):
            return 'conference'
        else:
            return kind

    def execute_insert_sql(connection_: pymysql.connections.Connection, sql_: str):
        with connection_.cursor() as cursor:
            try:
                cursor.execute(sql_)
                connection_.commit()
            except Exception as e:
                connection_.rollback()
                logger.error(f'Meet an error: {e=}, {type(e)=}, when executing sql query "{sql_}"')
                raise

    def select_last_insert_id(connection_: pymysql.connections.Connection) -> int:
        with connection_.cursor() as cursor:
            cursor.execute('SELECT last_insert_id();')
            return cursor.fetchone()[0]

    with pymysql.connect(host=mysql_connect['host'],
                         user=mysql_connect['user'],
                         password=mysql_connect['password'],
                         database=mysql_connect['database']) as connection:
        df = pd.read_csv(csv_file)

        author_name = df.loc[1][0]
        need_disambiguation = '1' if need_disambiguation else '0'
        sql = f"INSERT IGNORE INTO author(name, title) VALUES ('{escape_string(author_name)}', '{escape_string(author_title)}');"
        execute_insert_sql(connection, sql)

        author_id = select_last_insert_id(connection)

        for row in df.itertuples():
            venue_name = row[6]
            venue_kind = get_venue_kind(row[5])
            sql = f"INSERT IGNORE INTO venue(name, kind) VALUES ('{escape_string(venue_name)}', '{escape_string(venue_kind)}');"
            execute_insert_sql(connection, sql)

            paper_title = row[3]
            sql = f"SELECT id FROM paper WHERE title='{escape_string(paper_title)}';"
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                if result:
                    paper_id = result[0]
                else:
                    paper_venue = venue_name
                    paper_year = row[4]
                    author_count = int(row[7])
                    sql = f"INSERT IGNORE INTO paper(title, venue, year, author_count) VALUES ('{escape_string(paper_title)}', '{escape_string(paper_venue)}', '{paper_year}', '{author_count}');"
                    execute_insert_sql(connection, sql)
                    paper_id = select_last_insert_id(connection)

            is_first_author = row[8]
            contribution = 'FIRST_AUTHOR' if is_first_author else 'PAPER_AUTHOR'

            sql = f"INSERT IGNORE INTO author_paper(aid, pid, contribution, need_disambiguation) VALUES ('{author_id}', '{paper_id}', '{escape_string(contribution)}', '{need_disambiguation}');"
            execute_insert_sql(connection, sql)


def main():
    connection_config = json.load(open('./ScholarDataset/config.json'))
    for data_dir in data_dirs:
        for university in os.listdir(data_dir):
            university_dir = data_dir + '/' + university
            titles_dir = os.listdir(university_dir)
            for title in titles_dir:
                authors_dir = university_dir + '/' + title
                for author in os.listdir(authors_dir):
                    author_dir = authors_dir + '/' + author
                    dis_arti = glob.glob(author_dir + '/' + need_disambiguation_pattern)
                    undis_arti = glob.glob(author_dir + '/' + dont_need_disambiguation_pattern)
                    if dis_arti:
                        insert_csv_into_mysql(dis_arti[0], True, title, connection_config)
                        logger.info(f'{author}需要消歧')
                    elif undis_arti:
                        insert_csv_into_mysql(undis_arti[0], False, title, connection_config)
                        logger.info(f'{author}不需要消歧')
                    else:
                        logger.warning(f'{author}找不到csv文件')


if __name__ == '__main__':
    main()
