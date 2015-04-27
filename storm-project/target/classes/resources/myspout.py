from storm import Spout, emit, log
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer


client = KafkaClient("ip-172-31-28-55.ec2.internal:6667")
consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])

def getData():	
	data = consumer.next().value
	return data

class MySpout(Spout):
    def nextTuple(self):
        data = getData()
        emit([data])

   
MySpout().run()
