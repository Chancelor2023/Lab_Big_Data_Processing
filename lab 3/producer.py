# %%
import socket
import time
from confluent_kafka import Producer
from datetime import datetime, timedelta


# %%
print(datetime.now().strftime("%H:%M"))
# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='gutenberg_stream'

with open('103.txt', 'r', encoding='utf-8') as f:
    for line in f:
        producer.produce(
            topic=topic,
            value=line.encode('utf-8')
        )
        time.sleep(1)

producer.flush()
producer.close()
