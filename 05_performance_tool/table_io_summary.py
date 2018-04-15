#!/usr/bin/env python
#-*- coding:utf-8 -*-

import pymysql
import argparse
import sys
import time

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
        except:
            is_alive = False

        return is_alive

    def fetchall(self, sql):
        """获取所有的数据"""
        rs = []
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchall()
        except Exception as e:
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
            pass
         
        if is_dict and rs:
            cols = [col[0] for col in cursor.description]
            rs = dict(zip(cols, rs))

        return rs


"""
每一个项的检查接口
"""
class DBIOSummary(object):
    """检查实例的参数配置"""

    def __init__(self):
        """"""
    def conn(self, host, port, user, passwd):
        """连接到数据库"""
        self.mt = MySQLTool(host, port, user, passwd)
        
        is_alived = self.mt.conn_server(is_dict=True)
        return is_alived

    def print_table_summary(self, database='', tables=[], interval=5):
        """打印出db每秒流量情况"""

        title = ('{DB} {TABLE} {COUNT_READ}, {COUNT_WRITE}, {COUNT_FETCH}'
               '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                    DB = 'DB'.rjust(20),
                    TABLE = 'TABLE'.rjust(20),
                    COUNT_READ = 'COUNT_READ/s'.rjust(18),
                    COUNT_WRITE = 'COUNT_WRITE/s'.rjust(18),
                    COUNT_FETCH = 'COUNT_FETCH/s'.rjust(18),
                    COUNT_INSERT = 'COUNT_INSERT/s'.rjust(18),
                    COUNT_UPDATE = 'COUNT_UPDATE/s'.rjust(18),
                    COUNT_DELETE = 'COUNT_DELETE/s'.rjust(18),
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
            WHERE OBJECT_SCHEMA = '{db}'
                AND OBJECT_NAME IN ('{tables}')
        '''.format(db = database,
                   tables = "', '".join(tables))

        pre_info = {}

        while True:
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
                    per_count_insert = round(sub_count_read / interval, 2) if sub_count_read > 0 else 0
                    per_count_update = round(sub_count_update / interval, 2) if sub_count_update > 0 else 0
                    per_count_delete = round(sub_count_delete / interval, 2) if sub_count_delete > 0 else 0

                    print ('{DB} {TABLE} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
                           '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                                DB = info['OBJECT_SCHEMA'].rjust(20),
                                TABLE = info['OBJECT_NAME'].rjust(20),
                                COUNT_READ = str(per_count_read).rjust(18),
                                COUNT_WRITE = str(per_count_write).rjust(18),
                                COUNT_FETCH = str(per_count_fetch).rjust(18),
                                COUNT_INSERT = str(per_count_insert).rjust(18),
                                COUNT_UPDATE = str(per_count_update).rjust(18),
                                COUNT_DELETE = str(per_count_delete).rjust(18),
                          ))
            else: #
                for info in summary:
                    print ('{DB} {TABLE} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
                           '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                                DB = info['OBJECT_SCHEMA'].rjust(20),
                                TABLE = info['OBJECT_NAME'].rjust(20),
                                COUNT_READ = str(info['COUNT_READ']).rjust(18),
                                COUNT_WRITE = str(info['COUNT_WRITE']).rjust(18),
                                COUNT_FETCH = str(info['COUNT_FETCH']).rjust(18),
                                COUNT_INSERT = str(info['COUNT_INSERT']).rjust(18),
                                COUNT_UPDATE = str(info['COUNT_UPDATE']).rjust(18),
                                COUNT_DELETE = str(info['COUNT_DELETE']).rjust(18),
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
python table_io_summary.py --user=root --password=root --host=127.0.0.1 --port=3307 --interval=5 --database=db1 --table=t1
python table_io_summary.py --user=root --password=root --host=127.0.0.1 --port=3307 --interval=5 --database=db1 --table=t1 --table=t2

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
    # 添加 MySQL host 参数
    parser.add_argument('--host', dest='host', required=True,
                      action='store', default=None,
                      help='instance host', metavar='host')
    # 添加 MySQL Port 参数
    parser.add_argument('--port', dest='port', required=True,
                      action='store', default=None, type=int,
                      help='instance host', metavar='port')
    # 添加 MySQL database 参数
    parser.add_argument('--database', dest='database', required=True,
                      action='store', default=None, type=str,
                      help='database Name', metavar='database name')
    # 添加 MySQL table 参数
    parser.add_argument('--table', dest='tables', required=True,
                      action='append', metavar='table',
                      help='database Name',)
    # 添加 MySQL collection interval 参数
    parser.add_argument('--interval', dest='interval', required=False,
                      action='store', default=5, type=int,
                      help='collection info interval', metavar='5')

    args = parser.parse_args()

    return args

def main():
    args = parse_args() # 解析传入参数

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    database = args.database
    tables = args.tables
    interval = args.interval

    # 记录参数日志
    print 'param: [user={user}] [password=***] [host={host}] [port={port}] [database={database}] [interval={interval}]'.format(
                    user=user,
                    host=host, port=port,
                    database=database,
                    tables=','.join(tables),
                    interval=interval,)

    print '----------------------------------------------------'
    print ''

    db_io_summary = DBIOSummary()
    db_io_summary.conn(host=host, port=port, user=user, passwd=password)
    db_io_summary.print_table_summary(database=database, tables=tables, interval=interval)

if __name__ == '__main__':
    main()
