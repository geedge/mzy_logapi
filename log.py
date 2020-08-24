
import os
import time
import logging
import configparser
from clickhouse_driver import Client
from kafka import KafkaConsumer


def Logger(log_name):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
    log_path = os.getcwd() + '/Logs/'
    log_name = log_path + rq + log_name + '.log'
    logfile = log_name
    fh = logging.FileHandler(logfile, mode='w+')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
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
    return kafHost, cliHost, table, topic


def kafka_consumer(topic, host):
    """
    connecting to kafka broker
    """
    consumer = KafkaConsumer(topic, bootstrap_servers=host)
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
