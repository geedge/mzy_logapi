
from log import save_log, kafka_consumer, clic_client, readIni, log_init

kafHost, cliHost, status_table, status_topic, status_group = readIni(1)
logger = log_init("status_log")

try:
    client = clic_client(cliHost)
except:
    logger.error("clickhouse connect failed")

try:
    consumer_status = kafka_consumer(status_topic, status_group, kafHost)
except:
    logger.error("kafka connect failed")

for msg in consumer_status:
    try:
        save_log(eval(msg.value), status_table, client)
    except:
        logger.error("saving error")


