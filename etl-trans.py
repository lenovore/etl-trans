#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ConfigParser
import time
import datetime
import argparse
import sys
import logging
import os
import gc
import redis
import pymysql
import signal
from pymysqlreplication.event import QueryEvent, XidEvent
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, TableMapEvent
from pymysqlreplication import BinLogStreamReader

reload(sys)
sys.setdefaultencoding('utf8')

logger = logging.getLogger("mylogger")

class my_mysql(object):
    def __init__(self):
        self.ip = cnf['save_server']['host']
        self.user = cnf['save_server']['user']
        self.password = cnf['save_server']['passwd']
        self.db = cnf['save_server']['db']
        self.port = int(cnf['save_server']['port'])
        self.con = object

    def __enter__(self):
        self.con = pymysql.connect(
            host=self.ip,
            user=self.user,
            passwd=self.password,
            db=self.db,
            charset='utf8mb4',
            port=self.port
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def execute(self, sql=None):
        with self.con.cursor() as cursor:
            sqllist = sql
            cursor.execute(sqllist)
            result = cursor.fetchall()
            self.con.commit()
        return result

    def execute_bluk(self, sqllist=None):
        result = None
        with self.con.cursor() as cursor:
            try:
                for sql in sqllist:
                    cursor.execute(sql)
            except Exception as e:
                self.con.rollback()
            else:
                result = cursor.fetchall()
                self.con.commit()
        return result

class my_redis:
    def __init__(self):
        self.host = cnf['redis_server']['host']
        self.port = int(cnf['redis_server']['port'])
        self.passwd = cnf['redis_server']['passwd']
        self.log_pos_prefix = cnf['redis_server']['log_pos_prefix']
        self.server_id = cnf['master_server']['server_id']
        self.key = "{0}{1}".format(self.log_pos_prefix, self.server_id)
        self.connection = self.redis_conn()

    def redis_conn(self):
        try:
            self.connection = redis.StrictRedis(
                host=self.host, port=int(self.port), password=self.passwd)
            return self.connection
        except Exception as error:
            message = 'redis connect fail %s' % (error)
            logger.error(message)
            exit(1)

    def get_log_pos(self):
        try:
            ret = self.redis_conn().hgetall(self.key)
            return ret.get('log_file'), ret.get('log_pos')
        except Exception as error:
            logger.error("从redis 读取binlog pos点错误")
            logger.error(error)
            exit(1)

    def set_log_pos(self, *args):
        try:
            if args[0] == 'slave':
                self.redis_conn().hmset(
                    self.key, {'log_pos': args[2], 'log_file': args[1]})
            elif args[0] == 'master':
                self.redis_conn().hmset(self.key, {
                                'master_host': args[1], 'master_port': args[2], 'relay_master_log_file': args[3], 'exec_master_log_pos': args[4]})
        except Exception as error:
            logger.error("binlog pos点写入redis错误")
            logger.error(error)
            exit(1)

class mark_log:
    def __init__(self):
        try:
            self.file = cnf['binlog_position']['file']
        except Exception as error:
            logger.error("%s 获取存放pos点的文件错误." % (error))
            exit(1)
        self.open_file()

    def open_file(self):
        try:
            self.config = ConfigParser.ConfigParser()
            self.config.readfp(open(self.file, 'rw'))
        except Exception as error:
            logger.error("%s 文件打开错误" % (file))
            logger.error(error)
            exit(1)

    def get_log_pos(self):
        try:
            return self.config.get('binlog_position', 'filename'), int(self.config.get('binlog_position', 'position')),
        except Exception as error:
            logger.error("从文件 读取binlog pos点错误")
            logger.error(error)
            sys.exit(1)

    def set_log_pos(self, *args):
        try:
            if args[0] == 'slave':
                self.config.set('binlog_position', 'filename', args[1])
                self.config.set('binlog_position', 'position', str(args[2]))
                self.config.write(open(self.file, 'w'))
            elif args[0] == 'master':
                self.config.set('binlog_position', 'master_host', args[1])
                self.config.set('binlog_position', 'master_port', args[2])
                self.config.set('binlog_position', 'master_filename', args[3])
                self.config.set('binlog_position',
                                'master_position', str(args[4]))
                self.config.write(open(self.file, 'w'))
        except Exception as error:
            self.config.write(open(self.file, 'w'))
            logger.error("binlog pos点写入文件错误")
            logger.error(error)
            exit(1)

def binlog_reading():
    logger.info("开始ETL时间 %s" %
                (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    if logtoredis:
        objredis = my_redis()
        logger.info("同步binlog pos点从Redis读取")
    else:
        logger.info("同步binlog pos点从文件读取")
        objredis = mark_log()
    log_file, log_pos = objredis.get_log_pos()
    if log_file and log_pos:
        log_file = log_file
        log_pos = log_pos
    else:
        logger.error("获取binlog pos点错误,程序退出....")
        exit(1)
    logger.info("读取binlog: {0}:{1}".format(log_file, log_pos))

    trans_limit = int(cnf['limit']['sec'])
    mysql_conf = {}
    mysql_conf['host'] = cnf['master_server']['host']
    mysql_conf['port'] = int(cnf['master_server']['port'])
    mysql_conf['user'] = cnf['master_server']['user']
    mysql_conf['passwd'] = cnf['master_server']['passwd']
    server_id = int(cnf['master_server']['server_id'])
    stream = BinLogStreamReader(connection_settings=mysql_conf, server_id=server_id, log_file=log_file, log_pos=int(log_pos),
                                resume_stream=True, blocking=True, fail_on_table_metadata_unavailable=True, slave_heartbeat=10)
    begin = 0
    begintime = ""
    begin_pos = 0
    binlog_format = ""
    event_list = []
    event = {}
    try:
        for binlog_event in stream:
            # 从query: BEGIN事件开始，XidEvent为结束来判断事务执行的时间
            if isinstance(binlog_event, QueryEvent):
                query = binlog_event.query.lower()
                if query == "begin":
                    begin = 1
                    # 记录事务起始位置
                    begin_pos = stream.log_pos
                    continue
            '''
            从QueryEvent下的第一个Event来获取信息(有可能是包含注释的event，要排除)
            如果是TableMapEvent，则认为binlog_format=row；如果是QueryEvent，则认为是非row格式
            mariadb中query: BEGIN的事件中的日期是事务的结束时间，mysql中是开始时间，所以统一从QueryEvent下的第一个Event来获取事务开始时间
            '''
            if begin:
                # if isinstance(binlog_event, QueryEvent) and binlog_event.query.startwith("#"):
                # 非row模式下，通过判断事件是否有库来排除注释性的事件
                if not binlog_event.schema:
                    continue
                begin = 0
                begintime = binlog_event.timestamp
                schema = binlog_event.schema
                if isinstance(binlog_event, TableMapEvent):  # row
                    binlog_format = 'row'
                    table = binlog_event.table
                elif isinstance(binlog_event, QueryEvent):  # not row
                    binlog_format = 'statement'
                    table = ''
            # 不同格式的event提取数据的方式不同
            if binlog_format == 'row':
                if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent):
                    for row in binlog_event.rows:
                        if isinstance(binlog_event, WriteRowsEvent):
                            event["action"] = "insert"
                            event["values"] = row["values"]
                        elif isinstance(binlog_event, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["values"] = row["values"]
                        elif isinstance(binlog_event, UpdateRowsEvent):
                            event["action"] = "update"
                            event["before_values"] = row["before_values"]
                            event["after_values"] = row["after_values"]
                    event_list.append(str(event).replace("'","''"))
            elif binlog_format == 'statement':
                if isinstance(binlog_event, QueryEvent) and binlog_event.schema:
                    event_list.append(str(binlog_event.query).replace("'","''"))
            # XidEvent事件为事务结束
            if isinstance(binlog_event, XidEvent):
                endtime = binlog_event.timestamp
                transtime = endtime - begintime
                if transtime > trans_limit:
                    # 数据保存到mysql(会保存SQL或变更数据)
                    if savetomysql:
                        with my_mysql() as f:
                            '''
                            create table long_transaction
                            (
                                id int primary key auto_increment,
                                db varchar(50),
                                cost_second int,
                                start_time datetime,
                                end_time datetime,
                                bin_file varchar(20),
                                start_pos bigint,
                                end_pos bigint,
                                querys longtext
                            )
                            '''
                            sql = """
                                insert into long_transaction(db,cost_second,start_time,end_time,bin_file,start_pos,end_pos,querys)
                                values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')
                                """.format(
                                    schema,
                                    str(transtime),
                                    time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(begintime)),
                                    time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(endtime)),
                                    stream.log_file,
                                    begin_pos,
                                    stream.log_pos,
                                    ','.join(event_list)
                                )
                            f.execute(sql)
                        pass
                    else: # 默认保存到日志
                        logger.info("有超过{0}秒事务(耗时: {1}秒, time: {2}->{3}, binlog: {4}, pos: {5}->{6}, schema: {7})".format(
                                        str(trans_limit),
                                        str(transtime),
                                        time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(begintime)),
                                        time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(endtime)),
                                        stream.log_file,
                                        begin_pos,
                                        stream.log_pos,
                                        schema
                                    )
                                )
                objredis.set_log_pos('slave', stream.log_file, stream.log_pos)
                event_list = []
                event = {}
                binlog_format = ''
                begin=0
                gc.collect()
    except KeyboardInterrupt:
        log_file,log_pos = objredis.get_log_pos()
        logger.error("程序退出,当前同步位置 {0}:{1}".format(log_file,log_pos))
    except GracefulExitException:
        log_file,log_pos = objredis.get_log_pos()
        logger.error("程序被kill,当前同步位置 {0}:{1}".format(log_file,log_pos))
    except Exception as e:
        log_file,log_pos = objredis.get_log_pos()
        logger.error("程序异常：{2},当前同步位置 {0}:{1}".format(log_file,log_pos,str(e)))
    finally:
        stream.close()

def init_logging():
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(cnf['log']['log_dir'])
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(fh)
    logger.addHandler(sh)

def get_config(conf):
    try:
        if not os.path.exists(conf):
            print "指定的配置文件: %s 不存在" % (conf)
            exit(1)
        config = ConfigParser.ConfigParser()
        config.readfp(open(conf,'r'))
        config_dict={}
        for title in config.sections():
            config_dict[title]={}
            for one in config.options(title):
                config_dict[title][one]=config.get(title,one).strip(' ').strip('\'').strip('\"')
    except Exception as error:
        message="从配置文件获取配置参数错误: %s" % (error)
        print message
        exit(1)
    else:
        return config_dict

def init_parse():
    parser = argparse.ArgumentParser(description="ETL long time transactions from MySQL binlog",)
    parser.add_argument("-c", "--config", required=False, default='./meta.conf', help="Config file.")
    parser.add_argument('-r','--redis', action='store_true', default=False, help='log position to redis ,default file')
    parser.add_argument('-m','--mysql', action='store_true', default=False, help='big trans save to mysql ,default save to log file')
    return parser

class GracefulExitException(Exception):
    @staticmethod
    def sigterm_handler(signum, frame):
        raise GracefulExitException()
    pass

def sig():
    for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM, signal.SIGKILL]:
        signal.signal(sig, GracefulExitException.sigterm_handler)
        
if __name__ == "__main__":
    parser = init_parse()
    args = parser.parse_args()
    config = args.config
    logtoredis = args.redis
    savetomysql = args.mysql
    cnf=get_config(config)
    sig()
    init_logging()
    binlog_reading()
