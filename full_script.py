from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer

import numpy as np
from sklearn import svm
from sklearn.externals import joblib

import mysql.connector
from datetime import datetime

import json

count = 0
last20 = [0]*20
client = KafkaClient("ip-172-31-28-55.ec2.internal:6667")
consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])
#consumer = KafkaConsumer("shm", metadata_broker_list=['ip-172-31-28-55.ec2.internal:6667'])

conn = mysql.connector.connect(user='iotshm', password='pa$$word',
                              host='iotshm-data.ck3sx5qm0blx.us-west-2.rds.amazonaws.com',
                              database='iotshm')

cursor = conn.cursor()

#add_health = ("""INSERT IGNORE INTO iotshm.Health (sensor_id, timestamp, reading_type, healthy) VALUES (%s, %s, %s, %s)""")
add_magnitude = ("""INSERT IGNORE INTO iotshm.Magnitude (sensor_id, timestamp, reading_type, healthy, magnitude) VALUES(%s, %s, %s, %s, %s)""")


def getData():	
	data = consumer.next().value
	return data


def analyzeData(data):
	global count
	json_data = json.loads(data)

	sensor_id = json_data['sensorId']
	reading_type = json_data['readingType']

	time = json_data['time']	
	mag = json_data['mag']
	
  	filename = str(sensor_id) + "-0.pkl"
	clf = joblib.load(filename)
	pred = clf.predict(np.array(mag))

	healthy = True	
	if pred == -1:
		healthy = False
	
	data_magnitude = (sensor_id, time, reading_type, healthy, mag)
    	cursor.execute(add_magnitude, data_magnitude)

        #data_health = (sensor_id, time, reading_type, not unhealthy)
        #cursor.execute(add_health, data_health)
    	print(last20) 
   	conn.commit()
	if(healthy):
		last20[count%20] = 1
	else:
		last20[count%20] = 0
	print("number of healthies in the past 20 reads: " + str(len([x for x in last20 if count > 20 and x == 1])))
        count+=1
    	return "sensor_id: " + str(sensor_id) + "\ntime: " + str(time) + "\nreading_type: " + str(reading_type) + "\nmag:  " + str(mag) + "\nhealthy: " + str(healthy) + "\n"



while (True):
	data = getData()
    	print(analyzeData(data))


conn.close()



