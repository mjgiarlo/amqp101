import pika


conn = pika.AsyncoreConnection(pika.ConnectionParameters('localhost'))
channel = conn.channel()

# RPC pattern
# - client publishes to direct exchange specifying service as routing key, creating/subscribing to response queue
channel.basic_publish(exchange="direct",
                      routing_key="storage",
                      body="client -> storage (via direct)")
# - service picks message from queue, runs, sends response to response queue
# TODO, not sure how pika handles specifying response queues

# pub-sub pattern
# - bind multiple service queues to topic exchange using different filters
# DONE during service instantiation
# - client publishes to topic exchange
channel.basic_publish(exchange="topic",
                      routing_key="psu.curation.ingest",
                      body="client -> ingest/storage/identity (via topic)")

# pool/worker pattern
# - client publishes to direct exchange specifying ingest service as routing key, creating/subbing to resp queue
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [1]")
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [2]")
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [3]")
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [4]")
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [5]")
channel.basic_publish(exchange="direct",
                      routing_key="ingest",
                      body="client -> ingest (via pool) [6]")
# - client prints name of ingest service worker pulling message
# TODO, not sure how to get at this

# broadcast pattern
# - client publishes message to fanout exchange
channel.basic_publish(exchange="fanout",
                      routing_key="",
                      body="client -> all (via fanout)")
# - all services receive message

channel.close()
conn.close()
