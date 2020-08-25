
from log import save_log, kafka_consumer, clic_client, readIni, log_init

kafHost, cliHost, policy_table, policy_topic, policy_group = readIni(0)
logger = log_init("policy_log")

try:
    client = clic_client(cliHost)
except:
    logger.error("clickhouse connect failed")

try:
    consumer_policy = kafka_consumer(policy_topic, policy_group, kafHost)
except:
    logger.error("kafka connect failed")

for msg in consumer_policy:
    try:
        save_log(eval(msg.value), policy_table, client)
    except:
        logger.error("saving error")

