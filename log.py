
import os
import re
import logging
import configparser
from logging.handlers import TimedRotatingFileHandler
from clickhouse_driver import Client
from kafka import KafkaConsumer


def log_init(log_name):
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    LOG_PATH = os.getcwd() + '/Logs/' + log_name + "/"
    log_file_handler = TimedRotatingFileHandler(filename=LOG_PATH+"thread_", when="D", interval=1)
    # , backupCount=30)
    # log_file_handler.suffix = "%Y-%m-%d_%H-%M.log"
    # log_file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}.log$")
    log_file_handler.setFormatter(formatter)
    log_file_handler.setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.addHandler(log_file_handler)
    return logger


def readIni(i):
    """
    0: policy
    1: status
    """
    cf = configparser.ConfigParser()
    cf.read("config.ini")
    hosts = list(cf.items('hosts'))
    kafHost, cliHost = hosts[1][1], hosts[0][1]
    table = list(cf.items('tables'))[i][1]
    topic = list(cf.items('topics'))[i][1]
    group = list(cf.items('groups'))[i][1]
    return kafHost, cliHost, table, topic, group


def kafka_consumer(topic, groupid, host):
    """
    connecting to kafka broker
    """
    consumer = KafkaConsumer(topic,
                             group_id=groupid,
                             bootstrap_servers=host)
    return consumer


def clic_client(host):
    """
    connecting to clickhouse
    """
    client = Client(host=host)
    return client


def save_log(log, table, client):
    """
    log: status_log or policy log
    table: status/policy table name
    client: clickhouse client
    """
    key = ",".join(log.keys())
    value = tuple(log.values())
    client.execute("INSERT INTO {} ({}) VALUES {}".format(table, key, value))
