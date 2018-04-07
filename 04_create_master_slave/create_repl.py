#!/usr/bin/env python
#-*- coding:utf-8 -*-


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import argparse

reload(sys)
sys.setdefaultencoding('utf8')


class CreateRepl(object):
    """检测MySQL 主从状态是否符合要求"""
    
    master_conf = {} # MySQL 主库配置
    slave_conf = {}  # MySQL 从库配置

    master = None # Master 链接
    slave = None  # Slave 链接

    def __init__(self):
        pass

    def create_engine_str(self, username='root', password='root',
                          host='127.0.0.1', port=3306, database='',
                          charset='utf8'):
        """通过给的链接创建一个数据库链接串"""
        engine_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                      '?charset={charset}'.format(username = username,
                                                  password = password,
                                                  host = host,
                                                  port = port,
                                                  database = database,
                                                  charset = charset,))
        print engine_str
        return engine_str

    def create_session(self, username='root', password='root',
                       host='127.0.0.1', port=3306, database='',
                       charset='utf8'):
        """通过给的链接创建一个数据库Session"""
        engine_str = self.create_engine_str(username = username,
                                            password = password,
                                            host = host,
                                            port = port,
                                            database = database,
                                            charset = charset)
        engine = create_engine(engine_str)
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        return DBSession()

    def create_master_session(self, username='root', password='root',
                              host='127.0.0.1', port=3306, database='',
                              charset='utf8'):
        """创建MySQL Master Session"""
        print '\n=========================== Master Data Source Info =============================='

        self.master_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        print 'Master Conf:', self.master_conf
        self.master = self.create_session(**self.master_conf)


    def create_slave_session(self, username='root', password='root',
                              host='127.0.0.1', port=3306, database='',
                              charset='utf8'):
        """创建MySQL Master Session"""
        print '\n=========================== Slave Data Source Info =============================='

        self.slave_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        print 'slave Conf:', self.slave_conf
        self.slave = self.create_session(**self.slave_conf)

    def execute(self, session, sql):
        """执行SQL语句"""

        rs = session.execute(sql)

        return rs

    def bind_m_s(self, is_delay=False):
        """创建Master Slave"""

        delay_time = 21600 if is_delay else 0
        master_info = self.show_master_status()

        change_master_sql = '''
            CHANGE MASTER TO MASTER_HOST='{host}',
                MASTER_USER='{username}',
                MASTER_PASSWORD='{password}',
                MASTER_PORT={port},
                MASTER_DELAY={delay},
                MASTER_LOG_FILE='{binlog}',
                MASTER_LOG_POS={pos};       
        '''.format(
            host = self.master_conf['host'],
            port = self.master_conf['port'],
            username = self.master_conf['username'],
            password = self.master_conf['password'],
            binlog = master_info['File'],
            pos = master_info['Position'],
            delay = delay_time,
        )
        print 'Change Master TO SQL iS:'
        print change_master_sql
        print 'Start slave...'
        self.execute(self.slave, change_master_sql)
        self.execute(self.slave, 'start slave;')
        print 'slave start successful ...'
        print 'Master host: {host}, Port: {port}'.format(
                                           host = self.master_conf['host'],
                                           port = self.master_conf['port'])
        print 'Slave host: {host}, Port: {port}'.format(
                                           host = self.slave_conf['host'],
                                           port = self.slave_conf['port'])


    def show_master_status(self):
        """执行show master status语句"""
        sql = 'SHOW MASTER STATUS;'
        rs = self.execute(self.master, sql)
        row = rs.fetchone()
        rs_dict = dict(zip(rs.keys(), row))
        return rs_dict

def get_instance_info(instance, sep=':'):
    info = instance.split(sep)
    host = None
    port = 0
    is_delay = False
    if len(info) == 2:
        host, port = info
    elif len(info) == 3:
        host, port, typ = info
        if typ.lower() == 'delay':
            is_delay = True
    return host, int(port), is_delay

def get_conf(host, port):
    conf = {
        'username': 'HH',
        'password': 'oracle12',
        'host': host,
        'port': port,
    }
    return conf

def create_m_s(topology):
    for master, slaves_topology in topology.iteritems():
        slaves = slaves_topology.keys()

        for slave in slaves:
            master_info = get_instance_info(master) # master: host, port, is_delay
            slave_info = get_instance_info(slave) # master: host, port, is_delay
            master_conf = get_conf(master_info[0], master_info[1])
            slave_conf = get_conf(slave_info[0], slave_info[1])
            create_repl = CreateRepl()
            create_repl.create_master_session(**master_conf)
            create_repl.create_slave_session(**slave_conf)
            create_repl.bind_m_s(is_delay = slave_info[2])
        create_m_s(slaves_topology)


def main():
    topology = {
        '127.0.0.1:3307': {
            '127.0.0.1:3308': {
                '127.0.0.1:3309': {},
            },
        },
    }

    create_m_s(topology)


if __name__ == '__main__':
    main()
