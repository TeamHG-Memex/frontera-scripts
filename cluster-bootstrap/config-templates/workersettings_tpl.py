from crawlfrontier.settings.default_settings import MIDDLEWARES
from logging import INFO, DEBUG

MAX_REQUESTS = 0
MAX_NEXT_REQUESTS = 4096
CONSUMER_BATCH_SIZE = 1024
NEW_BATCH_DELAY = 5.0


#--------------------------------------------------------
# Url storage
#--------------------------------------------------------
BACKEND = 'crawlfrontier.contrib.backends.hbase.HBaseBackend'
HBASE_DROP_ALL_TABLES = False
HBASE_THRIFT_PORT = 9090
HBASE_THRIFT_HOST = [{thrift_servers_list}]
HBASE_QUEUE_PARTITIONS = {partitions_count}
HBASE_METADATA_TABLE = 'metadata'

MIDDLEWARES.extend([
    'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
    'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
])

KAFKA_LOCATION = '{kafka_location}:9092'
FRONTIER_GROUP = 'scrapy-crawler'
INCOMING_TOPIC = 'frontier-done'
OUTGOING_TOPIC = 'frontier-todo'
SCORING_GROUP = 'scrapy-scoring'
SCORING_TOPIC = 'frontier-score'

KAFKA_LOCATION_HH = '52.24.7.16:9092'
FRONTERA_INCOMING_TOPIC = 'broadcrawler-frontera-input'
FRONTERA_RESULTS_TOPIC = 'broadcrawler-output'

ZOOKEEPER_LOCATION = '{zookeeper_location}:2181'

from socket import getfqdn
JSONRPC_HOST=getfqdn()

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = True
LOGGING_BACKEND_ENABLED = True
LOGGING_BACKEND_LOGLEVEL = DEBUG
LOGGING_DEBUGGING_ENABLED = False


