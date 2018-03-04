#!/usr/bin/env python
#-*- coding:utf-8 -*-

import pymysql
import argparse
import sys
import os
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

    def close(self):
        if self.conn: self.conn.close()

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
        rs = None
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchall()
        except:
            pass
         
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


def parse_args():
    """解析命令行传入参数"""
    usage = """
Usage Example: 
python batch-exec-sql.py --host-port="127.0.0.1:3306" --sql="show slave status"
python batch-exec-sql.py --host-port="127.0.0.1:3306" --host-port="127.0.0.1:3306" --sql="show slave status"
python batch-exec-sql.py --host-file="ip.txt" --sql="show master status"

Description:
    execute sql and output result
    """

    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage, 
                            formatter_class = argparse.RawTextHelpFormatter)

    # 添加 Project Name 参数
    parser.add_argument('--host-port', dest='host_ports', required=False,
                      action='append', default=None, metavar='[host:port]',
                      help='--host-ports can use multiple times')
    # 添加 MySQL Host 参数
    parser.add_argument('--host-file', dest='host_file', required = False,
                      action='store', default=None,
                      help='host:port in file and every line', metavar='host')
    # 添加 table 参数
    parser.add_argument('--sql', dest='sql', required=True,
                      action='store', type=str,
                      help='sql', metavar='sql')

    args = parser.parse_args()

    return args


class BatchExecSQL(object):
    def __init__(self):
        self.host_ports = []

    def parse_host_ports_by_hosts_str(self, host_ports=None):
        for host_port in host_ports:
            host, port = host_port.split(':')

            self.host_ports.append([host, int(port)])

    def parse_host_ports_by_file(self, name=None):
        if not name:
            return

        with open(name, 'r') as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                host, port = line.split(':')
                self.host_ports.append([host, int(port)])
                

    def execute_sql(self, user, passwd, host, port, sql):
        """只能执行单个 host port 的sql"""
        mysqltool = MySQLTool(user = user,
                              passwd = passwd,
                              host = host,
                              port = port,)

        mysqltool.conn_server()
        rs = mysqltool.fetchall(sql = sql)

        return rs

    def batch_execute_sql(self, sql=None):
        for host, port in self.host_ports:
            user = 'HH'
            passwd = 'oracle'

            rows = self.execute_sql(user = user,
                                    passwd = passwd,
                                    host = host,
                                    port = port,
                                    sql = sql)

            for row in rows:
                print row


def main():
    args = parse_args() # 解析传入参数

    host_ports = args.host_ports
    host_file = args.host_file
    sql = args.sql

    # 记录参数日志
    print ('param: [host_ports={host_ports}] [host_file={host_file}] [sql={sql}]\n'.format(
                    host_ports=host_ports, host_file=host_file, sql=sql))

    batch_exce_sql = BatchExecSQL()

    if host_ports > 0:
        # 遍历解析 ip 和 port 字符串
        batch_exce_sql.parse_host_ports_by_hosts_str(host_ports = host_ports)

    elif host_file:
        # 解析文件中红的 ip 和 port
        batch_exce_sql.parse_host_ports_by_file(name = host_file)

    # 遍历解析出来的ip 和port 并执行sql命令
    batch_exce_sql.batch_execute_sql(sql = sql)

if __name__ == '__main__':
    main()
