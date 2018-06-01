#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import os
import time

import getpass
import pymysql
import argparse
import paramiko
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

    def close(self):
        if self.conn: self.conn.close()

    def conn_server(self, is_dict=False):
        """链接数据库"""
        if is_dict:
            self.conf['cursorclass'] = pymysql.cursors.DictCursor

        try:
            self.conn = pymysql.connect(**self.conf)
        except:
            raise

    def fetchall(self, sql):
        """获取所有的数据"""
        rs = None
        cursor =  None
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchall()
        except:
            raise
        finally:
            if cursor:
                cursor.close()
         
        return rs


    def fetchone(self, sql):
        """获取所有的数据"""
        rs = None
        cursor =  None
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)
            rs = cursor.fetchone()
        except:
            raise
        finally:
            if cursor:
                cursor.close()
         
        return rs

    def execute_ddl(self, sql):
        """执行ddl"""
        cursor =  None
        try:
            cursor = self.conn.cursor()
            cnt = cursor.execute(sql)

            self.conn.commit()
        except:
            raise
        finally:
            if cursor:
                cursor.close()


def parse_args():
    """解析命令行传入参数"""
    usage = """
Usage Example: 
python drop-table.py --host="127.0.0.1" --port=3306 --username=root --password=root --database=test --table=t1

# 每次 truncate 文件大小为 1M
python drop-table.py \\
    --host="127.0.0.1" \\
    --port=3306 \\
    --username=root \\
    --password=root \\
    --database=test \\
    --table=t1 \\
    --truncate-size=1048576 \\
    --ssh-port=22

Description:
    用于删除大表
    """

    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage, 
                            formatter_class = argparse.RawTextHelpFormatter)

    # 添加 MySQL Host 参数
    parser.add_argument('--host', dest='host', required=True,
                      action='store', type=str, metavar='HOST',
                      help='指定实例IP')
    # 添加 MySQL Port 参数
    parser.add_argument('--port', dest='port', required=True,
                      action='store', type=int, metavar='PORT',
                      help='指定实例端口')
    # 添加 user 参数
    parser.add_argument('--username', dest='username', required=True,
                      action='store', type=str, metavar='USER',
                      help='链接数据库的用户')
    # 添加 database 参数
    parser.add_argument('--password', dest='password', required=True,
                      action='store', type=str, metavar='PASSWORD',
                      help='链接数据库的密码')
    # 添加 database 参数
    parser.add_argument('--database', dest='database', required=True,
                      action='store', type=str, metavar='DB',
                      help='需要操作的数据库')
    # 添加 Table 参数
    parser.add_argument('--table', dest='table', required=True,
                      action='store', type=str, metavar='TABLE',
                      help='需要操作的表')
    # 添加 Truncate 数据文件大小
    parser.add_argument('--truncate-size', dest='truncate_size', required=False,
                      action='store', type=int, default=1048576, metavar='TRUNCATE SIZE',
                      help='每次truncate硬链接大小, 默认1M')
    # 添加 MySQL Port 参数
    parser.add_argument('--ssh-port', dest='ssh_port', required=False,
                      action='store', type=int, default=22, metavar='SSH PORT',
                      help='SSH 链接的端口')

    args = parser.parse_args()

    return args


class SSHExecute(object):
    """SSH 远程执行命令"""

    @classmethod
    def execute(self, _host, _port, _user, _id_rsa, _cmd):
        """SSH执行命令"""
        pkey = paramiko.RSAKey.from_private_key_file(_id_rsa)

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(_host, _port, _user, pkey=pkey)

        stdin, stdout, stderr = ssh.exec_command(_cmd)

        stdout_info = stdout.read().strip()
        stderr_info = stderr.read().strip()

        ssh.close()

        return stdout_info, stderr_info


class DropTable(object):
    def __init__(self, _host, _port, _username, _password, _database, _table,
        _ssh_port=22, _truncate_size=1048576):

        # 数据库 信息
        self.master_host = _host
        self.master_port = _port
        self.master_username = _username
        self.master_password = _password
        self.database = _database
        self.table = _table
        self.instances = {}

        # ssh 信息
        self.ssh_user = getpass.getuser()
        self.ssh_port = _ssh_port
        self.ssh_id_rsa = '{home}/{name}'.format(home = os.path.expanduser('~'),
                                                 name = '.ssh/id_rsa')

        # truncate 文件信息
        self.truncate_size = _truncate_size

    def file_exists(self, _host, _file_path):
        """检测文件是否存在"""
        # 远程获取文件大小
        cmd = 'll {file_path} | wc -l'.format(file_path = _file_path)
        output, err = SSHExecute.execute(_host = _host,
                                         _port = self.ssh_port,
                                         _user = self.ssh_user,
                                         _id_rsa = self.ssh_id_rsa,
                                         _cmd = cmd)

        return True if int(output) > 0 else False

    def truncate_file(self, _host, _file_path, _file_size):
        """对文件进行 truncate 操作
        """

        truncate_loop_count = _file_size // self.truncate_size

        # 如果文件大小  小于 每次 truncate 的大小, 直接返回 truncate 成功
        if truncate_loop_count <= 0: 
            return True

        cmd = '''
            for i in `seq {loop_count} -1 1 ` ;
            do
                truncate -s -{truncate_size} {file_path}
            done
        '''.format(loop_count = truncate_loop_count,
                   truncate_size = self.truncate_size,
                   file_path = _file_path)

        output, err = SSHExecute.execute(_host = _host,
                                         _port = self.ssh_port,
                                         _user = self.ssh_user,
                                         _id_rsa = self.ssh_id_rsa,
                                         _cmd = cmd)
        if err:
            print err
            return False
        else:
            return True

    def rm_file(self, _host, _file_path):
        """SSH 远程删除文件"""

        cmd = 'rm -f {file_path}'.format(file_path = _file_path)

        output, err = SSHExecute.execute(_host = _host,
                                         _port = self.ssh_port,
                                         _user = self.ssh_user,
                                         _id_rsa = self.ssh_id_rsa,
                                         _cmd = cmd)

        if err:
            print err
            return False
        else:
            return True
        
    def find_cascade_host(self, _host, _port, _username, _password, _indent=0):
        """ 通过递归查询数据库 获取每个实例的slave
        使用 show slave hosts 来获取一个实例下面的 slave
        """

        # 打印 host 缩进
        if not _host or not _port:
            msg = ('{indent} 改实例有注册成slave, 但是没有获取到相关的 IP:PORT, '
                   '{host}:{port}'.format(indent = '-' * _indent * 4,
                                          host = _host,
                                          port = _port))
            print msg.strip()

            return
        else:
            msg = '{indent} {host}:{port}'.format(indent = '-' * _indent * 4,
                                                  host = _host,
                                                  port = _port)
            print msg.strip()

        # 将实例加入到dict中
        instance_key = '{host}:{port}'.format(host=_host, port=_port)
        self.instances[instance_key] = {
            'host': _host,
            'port': _port,
            'username': _username,
            'password': _password,
            'indent': _indent,
        }
        
        # 创建数据库操作实例
        mysql_tool = MySQLTool(host = _host, 
                               port = _port,
                               user = _username,
                               passwd = _password,)

        # 循环获取实例下面的 所有slave
        try:
            mysql_tool.conn_server(is_dict = True)

            sql = '/* drop table script */ SHOW SLAVE HOSTS'

            slaves = mysql_tool.fetchall(sql)
            mysql_tool.close()

            for slave in slaves:
                self.find_cascade_host(_host = slave['Host'],
                                       _port = slave['Port'],
                                       _username = _username,
                                       _password = _password,
                                       _indent = _indent + 1)
        except:
            raise
        
    def find_datadir(self):
        """获取所有实例的数据文件路径"""

        for key, instance in self.instances.iteritems():
            # 创建数据库操作实例
            mysql_tool = MySQLTool(host = instance['host'], 
                                   port = instance['port'],
                                   user = instance['username'],
                                   passwd = instance['password'],)

            # 循环获取实例下面的 所有slave
            try:
                mysql_tool.conn_server(is_dict = True)

                sql = "/* drop table script */ SHOW GLOBAL VARIABLES LIKE 'datadir'"

                datadir = mysql_tool.fetchone(sql)

                self.instances[key]['datadir'] = datadir['Value']
                self.instances[key]['ibd_file'] = '{datadir}/{database}/{table}.ibd'.format(
                    datadir = datadir['Value'],
                    database = self.database,
                    table = self.table)

                msg = '获取实例数据文件路径: {file_path}, [{host_port}] '.format(
                    file_path = self.instances[key]['ibd_file'],
                    host_port = key,)
                print msg
            except:
                raise

    def find_table_data_file_info(self):
        """通过 SSH 远程获取表的文件, 
        1. 检查文件是否存在
        2. 获取文件大小, 并保存到 dict 中
        3. 删除获取数据文件失败的实例
        """

        for key, instance in self.instances.iteritems():

            # 远程获取文件大小
            cmd = 'ls -l {name}'.format(name = instance['ibd_file'])
            output, err = SSHExecute.execute(_host = instance['host'],
                                             _port = self.ssh_port,
                                             _user = self.ssh_user,
                                             _id_rsa = self.ssh_id_rsa,
                                             _cmd = cmd)

            if err: # 远程获取数据文件出错
                msg = '表: {table}, 获取数据文件失败, 该实例将不做数据文件操作: [{host}], {name}'.format(
                    table = self.table,
                    host = key,
                    name = instance['ibd_file'])
                print msg

                print err

                sys.exit(1)
            else:
                # 获取数据文件大小
                file_size = output.split()[4]
                self.instances[key]['file_size'] = int(file_size)

                msg = '数据文件大小为: {size}, {name} [{host}]'.format(
                    host = key,
                    name = instance['ibd_file'],
                    size = file_size,)
                print msg

    def find_host_dist_free_size(self):
        """获取"""

        for key, instance in self.instances.iteritems():

            # 远程获取文件大小
            cmd = 'df -k {name}'.format(name = instance['ibd_file'])
            output, err = SSHExecute.execute(_host = instance['host'],
                                             _port = self.ssh_port,
                                             _user = self.ssh_user,
                                             _id_rsa = self.ssh_id_rsa,
                                             _cmd = cmd)

            if err: # 远程获取数据文件出错
                msg = '表: {table}, 获取数据文件所在目录空闲空间失败: [{host}]'.format(
                    table = self.table,
                    host = key,)
                print msg

                print err

                sys.exit(1)
            else:
                # 获取数据文件大小
                data = output.split('\n')[1]
                free_size = data.split()[3]
                self.instances[key]['free_size'] = int(free_size) * 1024

                msg = '磁盘空闲空间: {size}, [{host}]'.format(
                    host = key,
                    size = free_size)
                print msg

    def check_disk_space_enough(self):
        """检测磁盘空间是否充足
        公式: 2 * table_size + 20G
        多实例情况: (2 * table1_size) + (2 * table2_size) + ... + 20G
        """

        hosts_size = {} # 存放每个 ip 总空闲空间, 和 需要的空间

        # 循环每个实例, 计算每个 ip 所对应的空闲空间和总需要的空间
        for key, instance in self.instances.iteritems():
            host = instance['host']

            if hosts_size.has_key(host):
                hosts_size[host]['need_size'] += 2 * instance['file_size']
            else:
                hosts_size[host] = {}
                hosts_size[host]['need_size'] = 2 * instance['file_size']
                hosts_size[host]['free_size'] = instance['free_size']

                # 如果表大于 15G 需要加上 20G额外空间
                if instance['file_size'] > 15 * 1024 * 1024 * 1024:
                    hosts_size[host]['need_size'] += 20 * 1024 * 1024 * 1024 # 初始化需要 20G 空间
                else: # 如果表小于 15G 可以不需要加上 2G 额外空间
                    hosts_size[host]['need_size'] += 2 * 1024 * 1024 * 1024 # 初始化需要 2G 空间

                # 相关实例信息
                hosts_size[host]['instances'] = {}
                hosts_size[host]['instances'][key] = copy.deepcopy(instance)

        # 需要检测 剩余空间是否大于需要的空间
        for host, host_size in hosts_size.iteritems():
            # 需要的空间 大于 空闲空间
            if host_size['need_size'] > host_size['free_size']:
                msg = ('失败, 空间检测. [{host}], 空闲空间: {free_size}, '
                       '需要空间: {need_size}. 该 IP 存在实例个数: {instance_size}'.format(
                          host = host,
                          free_size = host_size['free_size'],
                          need_size = host_size['need_size'],
                          instance_size = len(host_size['instances'])))
                print msg

                for key, instance in host_size['instances'].iteritems():
                    msg = '    {host}, 表大小: {file_size}'.format(host = key,
                                                                   file_size = instance['file_size'])
                    print msg

                sys.exit(1)
            else:
                msg = ('成功, 空间检测. [{host}], 空闲空间: {free_size}, '
                       '需要空间: {need_size}. 该 IP 存在实例个数: {instance_size}'.format(
                          host = host,
                          free_size = host_size['free_size'],
                          need_size = host_size['need_size'],
                          instance_size = len(host_size['instances'])))
                print msg

                for key, instance in host_size['instances'].iteritems():
                    msg = '    {host}, 表大小: {file_size}'.format(host = key,
                                                                   file_size = instance['file_size'])
                    print msg


    def create_hard_link(self):
        """创建硬链接"""

        for key, instance in self.instances.iteritems():
            # 远程创建文件硬链接
            cmd = 'ln {ori_file} {ori_file}.hdlk'.format(ori_file = instance['ibd_file'])
            output, err = SSHExecute.execute(_host = instance['host'],
                                             _port = self.ssh_port,
                                             _user = self.ssh_user,
                                             _id_rsa = self.ssh_id_rsa,
                                             _cmd = cmd)

            if err: # 远程获取数据文件出错
                msg = '失败. 硬链接创建: {link_file}.hdlk [{host}]'.format(
                    link_file = instance['ibd_file'],
                    host = key,)
                print msg

                print err

                sys.exit(1)
            else:
                msg = '成功. 硬链接创建: {link_file}.hdlk [{host}]'.format(
                    link_file = instance['ibd_file'],
                    host = key,)
                print msg

    def drop_master_table(self):
        """删除master的表"""
        # 创建数据库操作实例
        mysql_tool = MySQLTool(host = self.master_host, 
                               port = self.master_port,
                               user = self.master_username,
                               passwd = self.master_password,
                               db = self.database)

        sql = 'DROP TABLE IF EXISTS `{schema}`.`{table}`'.format(
            schema = self.database,
            table = self.table)

        try:
            mysql_tool.conn_server()
            mysql_tool.execute_ddl(sql)

            msg = '成功. 删除表 `{schema}`.`{table}` [{host}:{port}]'.format(
                schema = self.database,
                table = self.table, 
                host = self.master_host,
                port = self.master_port)

            print msg
        except:
            msg = '失败. 删除表 `{schema}`.`{table}` [{host}:{port}]'.format(
                schema = self.database,
                table = self.table, 
                host = self.master_host,
                port = self.master_port)

            print msg
            raise

    def remove_hard_link_file(self):
        """截取硬连接文件"""

        # 保存还没删除的表的实例 host:port 
        undrop_table_hosts = {key: True for key in self.instances}

        while undrop_table_hosts:

            # 重新拷贝一份需要删除文件的 host 列表, 用于循环使用
            tmp_undrop_table_hosts = copy.deepcopy(undrop_table_hosts)

            # 循环实例 truncate 硬连接
            for key in tmp_undrop_table_hosts:

                instance = self.instances[key]
                # 检测文件是否存在
                exists = self.file_exists(_host = instance['host'],
                                          _file_path = instance['ibd_file'])
                
                if exists: # 文件还存在
                    msg = ('ibd 文件还存在, 代表还没被删除. 稍后再删除 '
                           '{ibd_file}. [{host}]'.format(
                        ibd_file = instance['ibd_file'],
                        host = key))

                    undrop_table_hosts.append(key)

                    print msg
                else: # 文件不存在, 就 truncate 硬链接文件文件, 并 rm
                    file_path = '{ibd_file}.hdlk'.format(ibd_file = instance['ibd_file'])

                    # truncate 文件
                    ok = self.truncate_file(_host = instance['host'],
                                            _file_path = file_path,
                                            _file_size = instance['file_size'])
                    if not ok:
                        msg = ('失败. truncate 文件, 等下一次再进行: '
                               '{file_path}, [{host}]'.format(
                            file_path = file_path,
                            host = key))

                        print msg
                        continue
                    else:
                        msg = ('成功. truncate 文件: '
                               '{file_path}, [{host}]'.format(
                            file_path = file_path,
                            host = key))

                        print msg

                    # rm 文件
                    ok = self.rm_file(_host = instance['host'],
                                      _file_path = file_path)
                    if not ok:
                        msg = ('失败. 删除文件, 等下一次再进行: '
                               '{file_path}, [{host}]'.format(
                            file_path = file_path,
                            host = key))

                        print msg
                        continue
                    else:
                        msg = ('成功. 删除文件: '
                               '{file_path}, [{host}]'.format(
                            file_path = file_path,
                            host = key))

                        print msg


                    # 改实例的 硬链接文件已经删除, 不再需要进行删除
                    del undrop_table_hosts[key]
            
            
            
        
    def start(self):
        """开始进行删除表"""

        # 1. 递归获取, 所有的实例
        self.find_cascade_host(_host = self.master_host,
                               _port = self.master_port,
                               _username = self.master_username,
                               _password = self.master_password,
                               _indent = 0)
        print ''

        # 2. 循环实例, 获取所有的表的数据文件路径, 并且保存到一个 dict 中
        self.find_datadir()
        print ''

        # 3. 循环所有的实例, 并且通过 SSH 获取表的大小, 并映射到 dict 中
        self.find_table_data_file_info()
        print ''

        # 4. 循环获取实例机器空闲空间
        self.find_host_dist_free_size()
        print ''

        # 5. 检测空闲空间是否允许创建硬链接
        self.check_disk_space_enough()
        print ''

        # 6. 循环实例, 对每个 ibd 文件进行创建硬链接
        self.create_hard_link()
        print ''

        # 7. drop master table
        self.drop_master_table()
        print ''

        # 8. 循环 实例, 并且 SSH 执行 truncate 命令, 并且删除硬链接文件
        self.remove_hard_link_file()
        print ''


def main():
    args = parse_args() # 解析传入参数

    host = args.host
    port = args.port
    username = args.username
    password = args.password
    database = args.database
    table = args.table
    truncate_size = args.truncate_size
    ssh_port = args.ssh_port

    # 记录参数日志
    msg = '''
    输入参数为:
        host     : {host}
        port     : {port}
        username : {username}
        password : {password}
        database : {database}
        table    : {table}
        ssh_port : {ssh_port}
    '''.format(host = host,
               port = port,
               username = username,
               password = password,
               database = database,
               table = table,
               ssh_port = ssh_port)
    print msg

    drop_table = DropTable(_host = host,
                           _port = port,
                           _username = username,
                           _password = password,
                           _database = database,
                           _table = table,
                           _truncate_size = truncate_size,
                           _ssh_port = ssh_port)

    # 开始删除
    try:
        drop_table.start()
    except:
        raise


if __name__ == '__main__':
    main()
