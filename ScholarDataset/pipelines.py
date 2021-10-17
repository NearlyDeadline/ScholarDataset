# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql
import json
import pandas as pd
from pymysql.converters import escape_string
from scrapy.exceptions import DropItem
from ScholarDataset.items import ScholardatasetItem
import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("wos_pipeline_log.txt", encoding='utf-8')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class Author:
    abbr_name = ''
    full_name = ''
    university = ''
    college = ''
    email = ''
    contribution = ''


def is_same_title(expect_title: str, got_title: str) -> bool:
    cond = lambda c: str.isalpha(c)
    return ''.join(list(filter(cond, expect_title.lower()))) == ''.join(list(filter(cond, got_title.lower())))


def get_author_address_tuple(addresses: str) -> [(str, str)]:
    """
    :param addresses: str. Example: "[A, B; C, D; E, F] ADD1; [G, H] ADD2"
    :return: [name, address]. Example: [("A B", "ADD1"), ("C D", "ADD1"), ("E F", "ADD1"), ("G H", "ADD2")]
    """
    result = []
    name_addresses = [[names, address] for (names, address) in
                      [(j[0], j[1]) for j in [i.split('] ') for i in addresses[1:].split('; [')]]]
    for i in range(0, len(name_addresses)):
        result.extend([name.replace(',', ''), name_addresses[i][1]] for name in name_addresses[i][0].split('; '))
    return result


def get_corresponding_author(reprint_addresses: str) -> str:
    ra = reprint_addresses.split('(corresponding author)')
    if len(ra) == 1:
        ra = reprint_addresses.split('(Corresponding author)')
    if len(ra) > 1:
        return ra[0].replace(' ', '').replace(',', ' ')
    else:
        return ''


class ScholardatasetPipeline:
    def __init__(self):
        self.__connection_config = json.load(open('./ScholarDataset/config.json'))

    def open_spider(self, spider):
        self.__conn = pymysql.connect(user=self.__connection_config['user'],
                                      password=self.__connection_config['password'],
                                      host=self.__connection_config['host'],
                                      database=self.__connection_config['database'],
                                      charset='utf8mb4')
        self.__cursor = self.__conn.cursor()

    def process_item(self, item, spider):
        if type(item) == ScholardatasetItem:
            try:
                xls_df = pd.read_excel(item['content'])
                expect_title = item['query']
                got_title = xls_df['Article Title'][0]
                if not is_same_title(expect_title, got_title):
                    raise DropItem(f"对于'{expect_title}'，未能在Web of Science上找到题目完全一样的论文，只找到了'{got_title}'")

                sql = f"SELECT id FROM scholars.paper WHERE title = '{escape_string(expect_title)}';"
                self.__cursor.execute(sql)
                result = self.__cursor.fetchone()
                if not result:
                    raise DropItem(f"对于'{expect_title}'，执行'{sql}'语句时未在数据库中搜索到任何结果")
                paperid = result[0]
                # 根据paperid找出所有authorid，然后修改这些条目的contribution, email, affiliation, university等信息

                abbr_name_list = [s.replace(',', '') for s in xls_df['Authors'][0].split('; ')]
                full_name_list = [s.replace(',', '') for s in xls_df['Author Full Names'][0].split('; ')]
                address_list = get_author_address_tuple(str(xls_df['Addresses'][0]))
                email_list = [s for s in str(xls_df['Email Addresses'][0]).split('; ')]

                len_an = len(abbr_name_list)
                len_fn = len(full_name_list)
                len_ad = len(address_list)
                len_em = len(email_list)
                max_length = max(len_an, len_fn, len_ad, len_em)
                if len_ad < max_length:
                    for i in range(len_ad, max_length):
                        address_list.append('')
                if len_em < max_length:
                    for i in range(len_em, max_length):
                        email_list.append('')

                corresponding_author_name = get_corresponding_author(xls_df['Reprint Addresses'][0])

                author_list = []
                for i in range(0, len(abbr_name_list)):
                    author = Author()
                    author.abbr_name = abbr_name_list[i]
                    author.full_name = full_name_list[i]
                    author.email = email_list[i]
                    if corresponding_author_name == author.abbr_name:
                        author.contribution = 'CORRESPONDING_AUTHOR'
                    else:
                        author.contribution = 'PAPER_AUTHOR'
                    for name_address in address_list:
                        if name_address[0] == author.full_name:
                            address_list__ = name_address[1].split(', ')
                            author.university = address_list__[0]
                            author.college = address_list__[1] if len(address_list__) > 3 else ''
                    author_list.append(author)

                for a in author_list:
                    sql = f"SELECT aid FROM scholars.author_paper WHERE pid={paperid} and aid in (SELECT id from scholars.author WHERE name='{escape_string(a.full_name)}');"
                    self.__cursor.execute(sql)
                    result = self.__cursor.fetchone()
                    if not result:
                        names = a.full_name.split(' ')
                        if len(names) != 2:
                            continue
                        new_name = names[1] + ' ' + names[0]
                        sql = f"SELECT aid, contribution FROM scholars.author_paper WHERE pid={paperid} and aid in (SELECT id from scholars.author WHERE name='{escape_string(new_name)}');"
                        self.__cursor.execute(sql)
                        result2 = self.__cursor.fetchone()
                        if not result2:
                            logger.warning(f"未搜索到名字为'{new_name}'的作者，不会连锁更新其发表信息")
                            continue
                        else:
                            result = result2
                    authorid = result[0]
                    current_contribution = result[1]
                    if current_contribution != 'PAPER_AUTHOR':
                        a.contribution = current_contribution
                    sql = f"UPDATE author_paper SET contribution = '{escape_string(a.contribution)}', university = '{escape_string(a.university)}', college = '{escape_string(a.college)}', email = '{escape_string(a.email)}', need_disambiguation = 0 where aid = {authorid} and pid = {paperid};"
                    self.__cursor.execute(sql)
                    self.__conn.commit()
                    logger.info(f'成功执行更新语句："{sql}"')

            except Exception as e:
                logger.error(f"发生类型为{type(e)}的错误：'{e}'")
                raise

        return item

    def close_spider(self, spider):
        self.__cursor.close()
        self.__conn.close()
