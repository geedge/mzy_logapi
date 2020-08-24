
from log import save_log, kafka_consumer, clic_client, readIni, Logger

kafHost, cliHost, status_table, status_topic = readIni(1)
logger = Logger("status")

try:
    client = clic_client(cliHost)
except:
    logger.error("clickhouse connect failed")

try:
    consumer_status = kafka_consumer(status_topic, kafHost)
except:
    logger.error("kafka connect failed")

for msg in consumer_status:
    try:
        save_log(eval(msg.value), status_table, client)
    except:
        logger.error("saving error")


