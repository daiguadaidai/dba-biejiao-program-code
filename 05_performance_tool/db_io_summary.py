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
        except Exception as e:
            raise
         
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
        print is_alived
        return is_alived

    def print_db_summary(self, databases=[], interval=5):
        """打印出db每秒流量情况"""

        title = ('{DB} {COUNT_READ}, {COUNT_WRITE}, {COUNT_FETCH}'
               '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                    DB = 'DB'.rjust(25),
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
                SUM(COUNT_READ) AS COUNT_READ,
                SUM(COUNT_WRITE) AS COUNT_WRITE,
                SUM(COUNT_FETCH) AS COUNT_FETCH,
                SUM(COUNT_INSERT) AS COUNT_INSERT,
                SUM(COUNT_UPDATE) AS COUNT_UPDATE,
                SUM(COUNT_DELETE) AS COUNT_DELETE
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE OBJECT_SCHEMA IN('{dbs}')
            GROUP BY OBJECT_SCHEMA; 
        '''.format(dbs="', '".join(databases))

        pre_info = {}

        while True:
            summary = self.mt.fetchall(sql)

            if not summary:
                print 'Error, can`t find your databases[{dbs}]'.format(dbs = ','.join(databases))
                return

            print title

            if pre_info: # (当前采集信息 - 上次采集信息) / 采集间隔
                for info in summary:
                    sub_count_read = info['COUNT_READ'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_READ']
                    sub_count_write = info['COUNT_WRITE'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_WRITE']
                    sub_count_fetch = info['COUNT_FETCH'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_FETCH']
                    sub_count_insert = info['COUNT_INSERT'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_INSERT']
                    sub_count_update = info['COUNT_UPDATE'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_UPDATE']
                    sub_count_delete = info['COUNT_DELETE'] - pre_info[info['OBJECT_SCHEMA']]['COUNT_DELETE']

                    per_count_read = round(sub_count_read / interval, 2) if sub_count_read > 0 else 0
                    per_count_write = round(sub_count_write / interval, 2) if sub_count_write > 0 else 0
                    per_count_fetch = round(sub_count_fetch / interval, 2) if sub_count_fetch > 0 else 0
                    per_count_insert = round(sub_count_read / interval, 2) if sub_count_read > 0 else 0
                    per_count_update = round(sub_count_update / interval, 2) if sub_count_update > 0 else 0
                    per_count_delete = round(sub_count_delete / interval, 2) if sub_count_delete > 0 else 0

                    print ('{DB} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
                           '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                                DB = info['OBJECT_SCHEMA'].rjust(25),
                                COUNT_READ = str(per_count_read).rjust(18),
                                COUNT_WRITE = str(per_count_write).rjust(18),
                                COUNT_FETCH = str(per_count_fetch).rjust(18),
                                COUNT_INSERT = str(per_count_insert).rjust(18),
                                COUNT_UPDATE = str(per_count_update).rjust(18),
                                COUNT_DELETE = str(per_count_delete).rjust(18),
                          ))
            else: #
                for info in summary:
                    print ('{DB} {COUNT_READ} {COUNT_WRITE} {COUNT_FETCH}'
                           '{COUNT_INSERT} {COUNT_UPDATE} {COUNT_DELETE}'.format(
                                DB = info['OBJECT_SCHEMA'].rjust(25),
                                COUNT_READ = str(info['COUNT_READ']).rjust(18),
                                COUNT_WRITE = str(info['COUNT_WRITE']).rjust(18),
                                COUNT_FETCH = str(info['COUNT_FETCH']).rjust(18),
                                COUNT_INSERT = str(info['COUNT_INSERT']).rjust(18),
                                COUNT_UPDATE = str(info['COUNT_UPDATE']).rjust(18),
                                COUNT_DELETE = str(info['COUNT_DELETE']).rjust(18),
                          ))

            # 将本次的信息复制给上一次
            for info in summary:
                if not pre_info.has_key(info['OBJECT_SCHEMA']): # 该db之前不存在统计中
                    pre_info[info['OBJECT_SCHEMA']] = {}
                    pre_info[info['OBJECT_SCHEMA']]['OBJECT_SCHEMA'] = info['OBJECT_SCHEMA']

                pre_info[info['OBJECT_SCHEMA']]['COUNT_READ'] = info['COUNT_READ']
                pre_info[info['OBJECT_SCHEMA']]['COUNT_WRITE'] = info['COUNT_WRITE']
                pre_info[info['OBJECT_SCHEMA']]['COUNT_FETCH'] = info['COUNT_FETCH']
                pre_info[info['OBJECT_SCHEMA']]['COUNT_INSERT'] = info['COUNT_INSERT']
                pre_info[info['OBJECT_SCHEMA']]['COUNT_UPDATE'] = info['COUNT_UPDATE']
                pre_info[info['OBJECT_SCHEMA']]['COUNT_DELETE'] = info['COUNT_DELETE']

            print ''
            time.sleep(interval)
            

def parse_args():
    """解析命令行传入参数"""
    usage = """
Usage Example: 
python db_io_summary.py --host=127.0.0.1 --port=3307 --interval=5 --database=db1
python db_io_summary.py --host=127.0.0.1 --port=3307 --interval=5 --database=db1 --database=db2

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
    # 添加 MySQL database 参数
    parser.add_argument('--database', dest='databases', required=True,
                      action='append', metavar='project name',
                      help='Database Name')
    # 添加 MySQL database 参数
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
    databases = args.databases
    interval = args.interval

    # 记录参数日志
    print 'param: [user={user}] [password=***] [host={host}] [port={port}] [databases={databases}] [interval={interval}]'.format(
                    user=user,
                    host=host, port=port,
                    databases=databases,
                    interval=interval,)

    print '----------------------------------------------------'
    print ''

    try:
        db_io_summary = DBIOSummary()
        db_io_summary.conn(host=host, port=port, user=user, passwd=password)
        db_io_summary.print_db_summary(databases=databases, interval=interval)
    except Exception as e:
        raise

if __name__ == '__main__':
    main()
