#!/usr/bin/env python
#-*- coding:utf-8 -*-

import pymysql
import argparse
import sys
import time
import copy

reload(sys)
sys.setdefaultencoding('utf8')

"""
MySQL查询工具
"""
class MySQLTool(object):
    """对MySQL操作的一些命令"""

    def __init__(self, host, port, user, passwd, db='', charset='utf8'):
        self.conf = {
            'host': host,
            'port': int(port),
            'user': user,
            'passwd': passwd,
            'db': db,
            'charset': charset,
        }
        self.conn = None

    def conn_server(self, is_dict=False):
        """链接数据库"""
        if is_dict:
            self.conf['cursorclass'] = pymysql.cursors.DictCursor

        is_alive = False

        try:
            self.conn = pymysql.connect(**self.conf)
            is_alive = True
        except Exception as e:
            is_alive = False
            raise

        return is_alive

    def fetchall(self, sql):
        """获取所有的数据"""
        rs = []
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchall()
        except:
            raise
         
        return rs


    def fetchone(self, sql, is_dict=False):
        """获取所有的数据"""
        rs = None
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchone()
        except:
            raise
         
        if is_dict and rs:
            cols = [col[0] for col in cursor.description]
            rs = dict(zip(cols, rs))

        return rs


"""
每一个项的检查接口
"""
class TOPIOSummary(object):
    """检查实例的参数配置"""

    def __init__(self):
        """"""
    def conn(self, host, port, user, passwd):
        """连接到数据库"""
        self.mt = MySQLTool(host, port, user, passwd)
        
        is_alived = self.mt.conn_server(is_dict=True)
        return is_alived

    def print_table_summary(self, interval=5, top=10, by='COUNT_FETCH'):
        """打印出db每秒流量情况"""

        title = ('{DB} {TABLE} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
               '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                    DB = 'DB'.rjust(20),
                    TABLE = 'TABLE'.rjust(35),
                    COUNT_READ = 'COUNT_READ/s'.rjust(16),
                    COUNT_WRITE = 'COUNT_WRITE/s'.rjust(16),
                    COUNT_FETCH = 'COUNT_FETCH/s'.rjust(16),
                    COUNT_INSERT = 'COUNT_INSERT/s'.rjust(16),
                    COUNT_UPDATE = 'COUNT_UPDATE/s'.rjust(16),
                    COUNT_DELETE = 'COUNT_DELETE/s'.rjust(16),
              ))

        sql = '''
            SELECT
                OBJECT_SCHEMA,
                OBJECT_NAME,
                COUNT_READ,
                COUNT_WRITE,
                COUNT_FETCH,
                COUNT_INSERT,
                COUNT_UPDATE,
                COUNT_DELETE
            FROM performance_schema.table_io_waits_summary_by_table
        '''

        pre_info = {}

        while True:
            cache_infos = [] # 用来临时存储所有的平局信息, 后面会对该信息进行排序并输出 TopN
            summary = self.mt.fetchall(sql)

            if not summary:
                err_msg = 'Error, can`t find your database[{db}] and tables[{tables}]'.format(
                                 db = database,
                                 tables = ', '.join(tables))
                print err_msg
                return

            print title

            if pre_info: # (当前采集信息 - 上次采集信息) / 采集间隔
                for info in summary:
                    key = '{schema}.{table}'.format(schema = info['OBJECT_SCHEMA'],
                                                    table = info['OBJECT_NAME'])
                    sub_count_read = info['COUNT_READ'] - pre_info[key]['COUNT_READ']
                    sub_count_write = info['COUNT_WRITE'] - pre_info[key]['COUNT_WRITE']
                    sub_count_fetch = info['COUNT_FETCH'] - pre_info[key]['COUNT_FETCH']
                    sub_count_insert = info['COUNT_INSERT'] - pre_info[key]['COUNT_INSERT']
                    sub_count_update = info['COUNT_UPDATE'] - pre_info[key]['COUNT_UPDATE']
                    sub_count_delete = info['COUNT_DELETE'] - pre_info[key]['COUNT_DELETE']

                    per_count_read = round(sub_count_read / interval, 2) if sub_count_read > 0 else 0
                    per_count_write = round(sub_count_write / interval, 2) if sub_count_write > 0 else 0
                    per_count_fetch = round(sub_count_fetch / interval, 2) if sub_count_fetch > 0 else 0
                    per_count_insert = round(sub_count_insert / interval, 2) if sub_count_insert > 0 else 0
                    per_count_update = round(sub_count_update / interval, 2) if sub_count_update > 0 else 0
                    per_count_delete = round(sub_count_delete / interval, 2) if sub_count_delete > 0 else 0

                    # 将差值信息保存到 cache_infos 中
                    tmp_info = {}
                    tmp_info['OBJECT_SCHEMA'] = info['OBJECT_SCHEMA']
                    tmp_info['OBJECT_NAME'] = info['OBJECT_NAME']

                    tmp_info['COUNT_READ'] = per_count_read
                    tmp_info['COUNT_WRITE'] = per_count_write
                    tmp_info['COUNT_FETCH'] = per_count_fetch
                    tmp_info['COUNT_INSERT'] = per_count_insert
                    tmp_info['COUNT_UPDATE'] = per_count_update
                    tmp_info['COUNT_DELETE'] = per_count_delete

                    cache_infos.append(copy.deepcopy(tmp_info))

            else: #
                for info in summary:
                    # 将差值信息保存到 cache_infos 中
                    tmp_info = {}
                    tmp_info['OBJECT_SCHEMA'] = info['OBJECT_SCHEMA']
                    tmp_info['OBJECT_NAME'] = info['OBJECT_NAME']

                    tmp_info['COUNT_READ'] = info['COUNT_READ']
                    tmp_info['COUNT_WRITE'] = info['COUNT_WRITE']
                    tmp_info['COUNT_FETCH'] = info['COUNT_FETCH']
                    tmp_info['COUNT_INSERT'] = info['COUNT_INSERT']
                    tmp_info['COUNT_UPDATE'] = info['COUNT_UPDATE']
                    tmp_info['COUNT_DELETE'] = info['COUNT_DELETE']

                    cache_infos.append(copy.deepcopy(tmp_info))

            # 排序并且输出 TopN
            top_infos = sorted(cache_infos, key=lambda info: info[by], reverse=True)[:top]
            for top_info in top_infos:
                print ('{DB} {TABLE} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
                       '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                            DB = top_info['OBJECT_SCHEMA'].rjust(20),
                            TABLE = top_info['OBJECT_NAME'].rjust(35),
                            COUNT_READ = str(top_info['COUNT_READ']).rjust(16),
                            COUNT_WRITE = str(top_info['COUNT_WRITE']).rjust(16),
                            COUNT_FETCH = str(top_info['COUNT_FETCH']).rjust(16),
                            COUNT_INSERT = str(top_info['COUNT_INSERT']).rjust(16),
                            COUNT_UPDATE = str(top_info['COUNT_UPDATE']).rjust(16),
                            COUNT_DELETE = str(top_info['COUNT_DELETE']).rjust(16),
                      ))


            # 将本次的信息复制给上一次
            for info in summary:
                key = '{schema}.{table}'.format(schema = info['OBJECT_SCHEMA'],
                                                table = info['OBJECT_NAME'])
                if not pre_info.has_key(info['OBJECT_SCHEMA']): # 该db之前不存在统计中
                    pre_info[key] = {}
                    pre_info[key]['OBJECT_SCHEMA'] = info['OBJECT_SCHEMA']
                    pre_info[key]['OBJECT_NAME'] = info['OBJECT_NAME']

                pre_info[key]['COUNT_READ'] = info['COUNT_READ']
                pre_info[key]['COUNT_WRITE'] = info['COUNT_WRITE']
                pre_info[key]['COUNT_FETCH'] = info['COUNT_FETCH']
                pre_info[key]['COUNT_INSERT'] = info['COUNT_INSERT']
                pre_info[key]['COUNT_UPDATE'] = info['COUNT_UPDATE']
                pre_info[key]['COUNT_DELETE'] = info['COUNT_DELETE']

            print ''
            time.sleep(interval)
            

def parse_args():
    """解析命令行传入参数"""
    usage = """
Usage Example: 
python top_io_summary.py --host=127.0.0.1 --port=3307 --interval=5 --top=10
python top_io_summary.py --host=127.0.0.1 --port=3307 --interval=5 --top=10 --by=COUNT_FETCH

Description:
    Check the MySQL every instance is standard
    """

    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage, 
                            formatter_class = argparse.RawTextHelpFormatter)

    # 添加 MySQL 用户名 参数
    parser.add_argument('--user', dest='user', required=True,
                      action='store', default=None,
                      help='instance user', metavar='user')
    # 添加 MySQL 密码 参数
    parser.add_argument('--password', dest='password', required=True,
                      action='store', default=None,
                      help='instance password', metavar='password')
    # 添加 MySQL Host 参数
    parser.add_argument('--host', dest='host', required=True,
                      action='store', default=None,
                      help='instance host', metavar='host')
    # 添加 MySQL Port 参数
    parser.add_argument('--port', dest='port', required=True,
                      action='store', default=None, type=int,
                      help='instance host', metavar='port')
    # 添加 MySQL collection interval 参数
    parser.add_argument('--interval', dest='interval', required=False,
                      action='store', default=5, type=int,
                      help='collection info interval', metavar='5')
    # 添加 MySQL collection TopN 参数
    parser.add_argument('--top', dest='top', required=False,
                      action='store', default=10, type=int,
                      help='collection info top', metavar='10')
    # 添加 MySQL collection by 参数 是通过什么来进行排序
    parser.add_argument('--by', dest='by', required=False,
                      action='store', default='COUNT_FETCH', type=str,
                      choices=('COUNT_READ','COUNT_WRITE', 'COUNT_FETCH', 'COUNT_INSERT', 'COUNT_UPDATE', 'COUNT_DELETE'),
                      help='collection info by')

    args = parser.parse_args()

    return args

def main():
    args = parse_args() # 解析传入参数

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    interval = args.interval
    top = args.top
    by = args.by.upper()

    # 记录参数日志
    print 'param: [user={user}] [password=***] [host={host}] [port={port}] [top={top}] [by={by}]'.format(
                    user=user, 
                    host=host, port=port,
                    top=top, by=by,
                    interval=interval,)

    print '----------------------------------------------------'
    print ''

    try:
        top_io_summary = TOPIOSummary()
        top_io_summary.conn(host=host, port=port, user=user, passwd=password)
        top_io_summary.print_table_summary(interval=interval, top=top, by=by)
    except:
        raise

if __name__ == '__main__':
    main()
