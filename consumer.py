import pika
import asyncore


CONNECTION = pika.AsyncoreConnection(pika.ConnectionParameters('localhost'))
CHANNEL = CONNECTION.channel()


class MQService(object):
    def __init__(self, qname, topic, name=None):
        self.qname = qname
        self.topic = topic
        self.name = name if name else qname
        self.messages = []
        # create per-service queues
        # - bind each to direct exchange and fanout exchange
        CHANNEL.queue_declare(queue=self.qname, durable=True, exclusive=False, auto_delete=False)
        CHANNEL.queue_bind(queue=self.qname, exchange="direct", routing_key=self.qname)
        CHANNEL.queue_bind(queue=self.qname, exchange="topic", routing_key=self.topic)
        CHANNEL.queue_bind(queue=self.qname, exchange="fanout")
        self.subscribe(self.qname)
        #print "created service %s with queue %s" % (self.name, self.qname)
        return

    def __str__(self):
        s = []
        s.append(self.name)
        s.append("\n")
        for m in self.messages:
            s.append("\t")
            s.append(m)
            s.append("\n")
        return "".join(s)

    def publish(self, exchange, key, message):
        with MQChannel() as channel:
            channel.basic_publish(exchange=exchange,
                                  routing_key=key,
                                  body=message)
        #print "%s published %s to exchange %s with key %s" % (self.name, message, exchange, key)
        return

    def subscribe(self, queue):
        def handle_delivery(channel, method, header, body):
            import time
            print "%s received message %s in queue %s" % (self.name, body, queue)
            self.messages.append(body)
            CHANNEL.basic_ack(delivery_tag=method.delivery_tag)
            time.sleep(1)
        #print "%s subscribed to queue %s" % (self.name, queue)
        CHANNEL.basic_consume(handle_delivery, queue=queue)
        return


# create per-type exchanges
CHANNEL.exchange_declare(exchange="fanout", type="fanout", durable=True)
CHANNEL.exchange_declare(exchange="direct", type="direct", durable=True)
CHANNEL.exchange_declare(exchange="topic", type="topic", durable=True)

# create four services, some with multiple workers
identity = MQService(qname="identity", topic="psu.stewardship.services.*")
storage = MQService(qname="storage", topic="psu.stewardship.#")
audit = MQService(qname="audit", topic="#")
fixity = MQService(qname="fixity", topic="psu.stewardship.services.fixity")
ingest1 = MQService(qname="ingest", topic="psu.stewardship.services.ingest", name="ingest1")
ingest2 = MQService(qname="ingest", topic="psu.stewardship.services.ingest", name="ingest2")
ingest3 = MQService(qname="ingest", topic="psu.stewardship.services.ingest", name="ingest3")
ingest4 = MQService(qname="ingest", topic="psu.stewardship.services.ingest", name="ingest4")

asyncore.loop()

CHANNEL.close()
CONNECTION.close()
