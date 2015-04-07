from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer
import json

client = KafkaClient("ip-172-31-28-55.ec2.internal:6667")
consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])
#consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])

conn = mysql.connector.connect(user='iotshm', password='pa$$word',
                               host='iotshm-data.ck3sx5qm0blx.us-west-2.rds.amazonaws.com',
                               database='iotshm')

cursor = conn.cursor()

def getData():	
	data = consumer.next().value
	return data

def parseData(data):
    json_data=json.loads(data)
    sampling_freq = json_data['samplingFreq']
    sensor_id = json_data['sensorId']
    reading_type = json_data['readingType']
    mags = json_data['fftMags']
    fft_size = json_data['fftSize']
    output = str(sampling_freq) + ", " + str(sensor_id) + ", " + str(reading_type) + ", " + str(fft_size) + ", "
    for(x in mags):
        output += x + ", "
    return output[:-2]]

def writeData(outfile, parsed):
    f.write(parsed + "\n")
        
def main():
    f = open('json_class.csv', 'w')
    while (True):
        data = getData()
        parsed=parseData(data)
        writeData(f, parsed)
    conn.close()
    f.close()

if(__name__ == "__main__")
    main()
