# -*- coding: utf-8 -*-
# @Time    : 2021/10/16 20:26
# @Author  : Mike
# @File    : rank
import json
import pandas as pd
import pymysql
from multidict import CIMultiDict
from pymysql.converters import escape_string


def get_rank_dict(rid: int):
    connection_config = json.load(open('../ScholarDataset/config.json'))
    result = {}
    with pymysql.connect(host=connection_config['host'],
                         user=connection_config['user'],
                         password=connection_config['password'],
                         database=connection_config['database']) as connection:
        with connection.cursor() as cursor:
            sql = f"SELECT name FROM researcher WHERE id = {rid};"
            cursor.execute(sql)
            name = cursor.fetchall()
            if name:
                result['Researcher Name'] = name[0]
                result['Achievements'] = []
            else:
                return

            sql = f"SELECT title, venue, year, author_count, contribution FROM paper LEFT JOIN author_paper ON paper.id = author_paper.pid WHERE aid in (SELECT id FROM author WHERE rid = {rid});"
            count = cursor.execute(sql)
            for i in range(0, count):
                ac = {
                    'Paper Title': '',
                    'Contribution': '',
                    'Venue': {},
                    '汤森路透分区': {},
                    '中科院分区': {},
                    'CCF': {}
                }
                item = cursor.fetchone()
                ac['Paper Title'] = item[0]
                ac['Contribution'] = item[4]
                venue = item[1]
                ac['Venue'] = {get_venue(venue, connection): venue}

                pub_year = item[2]
                jcr_rank_dict = get_jcr_rank_dict(venue, pub_year)
                if jcr_rank_dict:
                    ac['汤森路透分区'] = jcr_rank_dict

                cas_rank_dict = get_cas_rank_dict(venue, pub_year)
                if cas_rank_dict:
                    ac['中科院分区'] = cas_rank_dict

                ccf_rank_dict = get_ccf_rank_dict(venue, pub_year)
                if ccf_rank_dict:
                    ac['CCF'] = ccf_rank_dict

                result['Achievements'].append(ac)

    return result


def get_venue(venue: str, mysql_connection: pymysql.connections.Connection) -> str:
    """
    :param venue: Venue名称
    :param mysql_connection: 数据库连接，需要创建一个新的游标，以免和之前冲突
    :return: Venue对应的Kind列的值
    """
    with mysql_connection.cursor() as cur:
        sql = f"SELECT kind FROM venue WHERE name = '{escape_string(venue)}';"
        cur.execute(sql)
        r = cur.fetchone()
    if r:
        return r[0]
    else:
        return ''


def get_ccf_rank_dict(venue: str, year: str) -> dict:
    """
    :param year 年份
    :param venue能有两种情况：

    (1)会议：DBLP文件里提取的简称，需要与ccf.csv的“DBLP简称”或“CCF简称”列对应

    (2)期刊：XLS文件的Source Title列，需要与ccf.csv的“全称”列对应。这里需无视大小写

    由于无法区分两种情况的值，所以依次搜索两列，凡可得到结果的情况就作为结果
    """
    if int(year) >= 2019:
        year = '2019'
    else:
        year = '2015'
    ccf_data = pd.read_csv(f'ccf_{year}.csv', header=0, index_col=[0])
    ccf_data.fillna('', inplace=True)

    if venue in ccf_data.index:  # 全称，直接访问索引
        target_row = ccf_data.loc[venue]
        ccf_rank_dict = {
            'CCF Abbr': target_row['CCF简称'],
            'Venue Full Name': target_row['全称'],
            'Field': target_row['领域'],
            'Rank': target_row['评级']
        }
        return ccf_rank_dict

    # 会议，依次搜索“DBLP简称”，“CCF简称”两列。这里不是访问索引，因此需要访问第0个元素
    target_row = ccf_data.loc[ccf_data['DBLP简称'] == venue]
    if target_row.empty:
        target_row = ccf_data.loc[ccf_data['CCF简称'] == venue.upper()]
        if target_row.empty:
            return {}

    ccf_rank_dict = {
        'CCF Abbr': target_row['CCF简称'][0],
        'Venue Full Name': target_row['全称'][0],
        'Field': target_row['领域'][0],
        'Rank': target_row['评级'][0]
    }
    return ccf_rank_dict


def get_jcr_rank_dict(venue: str, year: str) -> dict:
    """
    :param venue只有一种情况：期刊。直接查表即可
    :param year 年份，'2015'-'2020'之间的值
    """
    if int(year) >= 2020:
        year = '2020'
    elif int(year) < 2015:
        year = '2015'
    jcr_rank_dict = {}
    if not pd.isna(venue):
        jcr = json.load(open(f'jcr_{year}.json'))
        jcr = CIMultiDict(jcr)
        if jcr.get(venue):
            jcr_rank_dict = jcr.get(venue)
    return jcr_rank_dict


def get_cas_rank_dict(venue: str, year: str) -> dict:
    """
    :param venue只有一种情况：期刊。直接查表即可
    :param year 年份，'2015'-'2019'之间的值
    """
    if int(year) >= 2019:
        year = '2019'
    elif int(year) < 2015:
        year = '2015'
    cas_rank_dict = {}
    if not pd.isna(venue):
        cas = json.load(open(f'cas_{year}.json'))
        cas = CIMultiDict(cas)
        if cas.get(venue):
            cas_rank_dict = cas.get(venue)
    return cas_rank_dict


if __name__ == '__main__':
    rid = 3
    result = get_rank_dict(rid)
    json.dump(result, open(f'../data/{rid}.json', 'w', encoding='utf8'), indent=4, ensure_ascii=False)
