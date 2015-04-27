from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer
import json
import datetime


client = KafkaClient("ip-172-31-28-55.ec2.internal:6667")
consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])
#consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])

def getData():	
	data = consumer.next().value
	return data

def parseData(data):
    json_data=json.loads(data)
    sensor_id = json_data['sensorId']
    reading_type = json_data['readingType']
    mag = json_data['mag']
    output = str(sensor_id) + "," + str(reading_type) + "," + str(mag)
    return output

def writeData(outfile, parsed):
    outfile.write(parsed + "\n")
        
def main():
    f = open('mag2classify.csv', 'w')
    start = datetime.datetime.now().time()
    x = 0
    while (x < 43200):
        data = getData()
        #print(data)
        parsed=parseData(data)
        #print(parsed)
        writeData(f, parsed)
        #print(parsed)
        x+=1
        if x%30==0:
            print(x)
            print(parsed)
    f.close()

if(__name__ == "__main__"):
    main()
