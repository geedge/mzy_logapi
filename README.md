# 日志接口

## log.py
封装的公共模块

## read_policy.py

从kafka中读取安全策略日志，并存入clickhouse

## read_status.py

从kafka中读取状态日志，并存入clickhouse

## config.ini

配置文件，指定host，消费者groupid，clickhouse表名，kafka topic名

## 日志记录

异常记录在Logs文件夹下，每天产生一个日志文件
