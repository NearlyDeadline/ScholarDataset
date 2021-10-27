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
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("wos_pipeline_log.txt", encoding='utf-8')
handler.setLevel(logging.WARNING)
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
    lab = ''
    researcher_id = None


def is_same_title(expect_title: str, got_title: str) -> bool:
    """
    :param expect_title: 期待的目标题目
    :param got_title: 实际得到的题目
    :return: 如果其中一方以另一方为前缀，则返回True；否则返回False
    """
    cond = lambda c: str.isalpha(c)
    expect_chars = ''.join(list(filter(cond, expect_title.lower())))
    got_chars = ''.join(list(filter(cond, got_title.lower())))
    return expect_chars.startswith(got_chars) or got_chars.startswith(expect_chars)


def is_same_name(name1: str, name2: str) -> bool:
    """
    :param name1: 第一个名字
    :param name2: 第二个名字
    :return: 两名字相等返回True，否则False（注意姓和名的先后顺序不影响）
    """
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
    """
    :param reprint_addresses: xls_data['Reprint Addresses'][0]
    :return: 通讯作者的姓名
    """
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
        paper_id = item['paper_id']
        try:
            xls_df = pd.read_excel(item['content'])
            got_title = xls_df['Article Title'][0]
            if not is_same_title(expect_title, got_title):
                raise DropItem(f"未能在Web of Science上找到题目完全一样的论文，只找到了'{got_title}'。")

            # 计算姓名简称列表，全名列表，地址列表，邮箱列表
            abbr_name_list = [s.replace(',', '') for s in xls_df['Authors'][0].split('; ')]
            full_name_list = [s.replace(',', '') for s in xls_df['Author Full Names'][0].split('; ')]
            address_list = get_author_address_tuple(str(xls_df['Addresses'][0]))
            email_list = [s for s in str(xls_df['Email Addresses'][0]).split('; ')]

            # 在大多数论文中，这四个表一一对应，但少数情况下不是，因此需要下面的补全操作
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

            # 为每个Author填充姓名，机构，邮箱等基本信息；并识别通讯作者
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

            # 为每个Author填充researcher_id
            sql = f"SELECT id, name FROM researcher WHERE id in (SELECT rid FROM author WHERE id in (SELECT aid FROM author_paper WHERE pid = {paper_id}));"
            self.__cursor.execute(sql)
            result = self.__cursor.fetchall()
            for item in result:
                rid = item[0]
                # 如果作者姓名后面有个数字，把最后的数字部分去掉
                target_researcher_name = item[1] if not re.compile(r".*[0-9]$").match(item[1]) else ' '.join(
                    item[1].split(' ')[:-1])
                for author in author_list:
                    if is_same_name(author.full_name, target_researcher_name):
                        author.researcher_id = rid

            # UPDATE有rid的Author，INSERT无rid的Author
            for author in author_list:
                if author.researcher_id:
                    # 寻找已有的作者信息：包括邮箱、机构等，是否在表中
                    sql = "SELECT id FROM author WHERE rid={} AND email='{}' AND university='{}' AND college='{}' AND lab='{}';".format(
                        author.researcher_id,
                        escape_string(author.email),
                        escape_string(author.university),
                        escape_string(author.college),
                        escape_string(author.lab))
                    self.__cursor.execute(sql)
                    result = self.__cursor.fetchone()

                    if not result:  # 没有该作者信息，需要插入，并与researcher表建立联系
                        # 如果已有的数据无需消歧，新生成的作者自然也无需消歧，他们只是邮箱、机构信息不同而已。反之同理
                        sql = f"SELECT need_disambiguation from author WHERE rid ={author.researcher_id};"
                        self.__cursor.execute(sql)
                        need_disambiguation = self.__cursor.fetchone()[0]

                        sql = "INSERT INTO author(rid, email, university, college, lab, need_disambiguation) VALUES('{}', '{}', '{}', '{}', '{}', {});".format(
                            author.researcher_id,
                            escape_string(author.email),
                            escape_string(author.university),
                            escape_string(author.college),
                            escape_string(author.lab),
                            need_disambiguation
                        )
                        self.__cursor.execute(sql)
                        self.__conn.commit()
                        logger.info(f'成功建立新的Author：{sql}')
                        self.__cursor.execute('SELECT last_insert_id();')
                        author_id = self.__cursor.fetchone()[0]
                    else:  # 已有作者信息，直接取id并更新Author信息即可
                        author_id = result[0]
                        sql = "UPDATE author SET email= '{}', university = '{}', college = '{}', lab = '{}' WHERE id = {};".format(
                            escape_string(author.email),
                            escape_string(author.university),
                            escape_string(author.college),
                            escape_string(author.lab),
                            author_id
                        )
                        self.__cursor.execute(sql)
                        self.__conn.commit()
                        logger.info(f'成功执行更新语句："{sql}"')

                    # 更新author_id与贡献，初始情况aid的值为researcher_id，已有作者信息数据时aid为author_id
                    sql = f"SELECT contribution FROM author_paper WHERE (aid = {author.researcher_id} OR aid = {author_id}) AND pid={paper_id};"
                    self.__cursor.execute(sql)
                    current_contribution = self.__cursor.fetchone()[0]
                    if current_contribution != 'PAPER_AUTHOR':
                        author.contribution = current_contribution
                    sql = f"UPDATE author_paper SET aid = {author_id}, contribution = '{escape_string(author.contribution)}' WHERE aid = {author.researcher_id} AND pid = {paper_id};"
                    self.__cursor.execute(sql)
                    self.__conn.commit()
                    logger.info(f'成功执行更新语句："{sql}"')
                else:
                    pass
                    # 暂时不需要将未出现在DBLP列表里的作者插入数据库中

        except Exception as e:
            logger.error(
                f"发生类型为{type(e)}的错误：'{repr(e)}'。请检查pid={paper_id}，论文题目为{expect_title}。追踪位置：{traceback.format_exc()}。")
            raise

        return item

    def close_spider(self, spider):
        self.__cursor.close()
        self.__conn.close()
