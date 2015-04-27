import storm
import numpy as np
from sklearn import svm
from sklearn.externals import joblib

import mysql.connector
from datetime import datetime

import json

conn = mysql.connector.connect(user='iotshm', password='pa$$word',
                              host='iotshm-data.ck3sx5qm0blx.us-west-2.rds.amazonaws.com',
                              database='iotshm')

cursor = conn.cursor()

add_magnitude = ("""INSERT IGNORE INTO iotshm.MagnitudeV2 (sensor_id, timestamp, reading_type, healthy, magnitude) VALUES(%s, %s, %s, %s, %s)""")


def analyzeData(data):
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
       	
	conn.commit()

  return "sensor_id: " + str(sensor_id) + "\ntime: " + str(time) + "\nreading_type: " + str(reading_type) + "\nmag:  " + str(mag) + "\nhealthy: " + str(healthy) + "\n"



class MyBolt(storm.BasicBolt):
    def process(self, tup):
        
        data = tup.values[0]
        output = analyzeData(data)
        storm.emit([output])
        
		
MyBolt().run()


