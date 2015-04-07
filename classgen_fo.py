from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer

import numpy as np
from sklearn import svm
from sklearn.externals import joblib

import mysql.connector
from datetime import datetime

import json
import uuid


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
        freq_array = np.array((1 * sampling_freq / fft_size))
    for i in range(2, int(fft_size/2)):
        freq_i = np.array((i * sampling_freq / fft_size))
            freq_array = np.vstack((freq_array, freq_i))
    mags_list = mags_array.tolist()
    freq_list = freq_array.tolist()

def writeData(writeData):
    
while (True):
    open('json_class.csv','wb') as outfile
	data = getData()
    #set amount of time between writing data
    writeData(outfile, data)

conn.close()



