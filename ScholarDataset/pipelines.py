# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import pymysql
import json
import pandas as pd
from pymysql.converters import escape_string
from scrapy.exceptions import DropItem
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


def is_same_name(name1: str, name2: str) -> bool:
    return name1 == name2 or ' '.join(name1.split(' ')[::-1]) == name2


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
        expect_title = item['query']
        author_id = item['author_id']
        paper_id = item['paper_id']
        try:
            xls_df = pd.read_excel(item['content'])
            got_title = xls_df['Article Title'][0]
            if not is_same_title(expect_title, got_title):
                raise DropItem(f"对于'{expect_title}'，未能在Web of Science上找到题目完全一样的论文，只找到了'{got_title}'")

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

            sql = f"SELECT name FROM author WHERE id = {author_id};"
            self.__cursor.execute(sql)
            target_author_name = self.__cursor.fetchone()[0]
            # 如果作者姓名后面有个数字，把最后的数字部分去掉
            if re.compile(r".*[0-9]$").match(target_author_name):
                target_author_name = ' '.join(target_author_name.split(' ')[:-1])
            target_author_index = 0
            for author in author_list:
                if is_same_name(author.full_name, target_author_name):
                    break
                target_author_index += 1
            if target_author_index == len(author_list):
                raise DropItem(f"对于{expect_title}，未在Web of Science爬取结果中寻找到作者信息，作者id为{author_id}")

            sql = f"SELECT aid, contribution FROM author_paper WHERE aid = {author_id} AND pid={paper_id};"
            self.__cursor.execute(sql)
            current_contribution = self.__cursor.fetchone()[1]
            if current_contribution != 'PAPER_AUTHOR':
                author_list[target_author_index].contribution = current_contribution
            sql = f"UPDATE author_paper SET contribution = '{escape_string(author_list[target_author_index].contribution)}', university = '{escape_string(author_list[target_author_index].university)}', college = '{escape_string(author_list[target_author_index].college)}', email = '{escape_string(author_list[target_author_index].email)}' WHERE aid = {author_id} AND pid = {paper_id};"
            self.__cursor.execute(sql)
            self.__conn.commit()
            logger.info(f'成功执行更新语句："{sql}"')

        except Exception as e:
            logger.error(f"发生类型为{type(e)}的错误：'{e}'。请检查aid={author_id}, pid={paper_id}，论文题目为{expect_title}")
            raise

        return item

    def close_spider(self, spider):
        self.__cursor.close()
        self.__conn.close()
