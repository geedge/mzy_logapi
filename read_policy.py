
from log import save_log, kafka_consumer, clic_client, readIni, Logger

kafHost, cliHost, policy_table, policy_topic = readIni(0)
logger = Logger("policy")

try:
    client = clic_client(cliHost)
except:
    logger.error("clickhouse connect failed")

try:
    consumer_policy = kafka_consumer(policy_topic, kafHost)
except:
    logger.error("kafka connect failed")

for msg in consumer_policy:
    try:
        save_log(eval(msg.value), policy_table, client)
    except:
        logger.error("saving error")

