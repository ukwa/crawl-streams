from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer, TopicPartition

topic  = "fc.crawled" 
broker = "192.168.45.15:9094"

# lets check messages of the first day in New Year
date_out = datetime.now(tz=timezone.utc) - timedelta(hours=1)
date_in  = date_out - timedelta(hours=1)
print(date_in, date_out)

consumer = KafkaConsumer(topic, bootstrap_servers=broker, enable_auto_commit=True, consumer_timeout_ms=30000)

tps = consumer.partitions_for_topic(topic)
print(tps)
timemap_in = {}
timemap_out = {}
for partition in tps:
  tp = TopicPartition(topic, partition)
  timemap_in[tp] = date_in.timestamp() * 1000
  timemap_out[tp] = date_out.timestamp() * 1000

# in fact you asked about how to use 2 methods: offsets_for_times() and seek()
rec_in  = consumer.offsets_for_times(timemap_in)
rec_out = consumer.offsets_for_times(timemap_out)

for partition in tps:
  tp = TopicPartition(topic, partition)
  consumer.seek(tp, rec_in[tp].offset) # lets go to the first message in New Year!

c = 0
oos = set()
for msg in consumer:
  if msg.offset >= rec_out[TopicPartition(topic, msg.partition)].offset:
    oos.add(msg.partition)
  else:
    c += 1
  if len(oos) == len(tps):
    break

print("{c} messages between {_in} and {_out}".format(c=c, _in=str(date_in), _out=str(date_out)))

