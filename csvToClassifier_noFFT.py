import json
import datetime
import csv
import numpy as np
from sklearn import svm
from sklearn.externals import joblib

def main():
    d={}
    with open('mag2classify.csv', 'rU') as f:
        print('opening csv')
        reader=csv.reader(f)
        for row in reader:
            nprow=np.array(row)
            sensor_id=row[0].replace(' ','')
            reading_type=row[1].replace(' ', '')
            mag=nprow[2]
            if sensor_id not in d:
                d[sensor_id]={}
                print(sensor_id)
            if reading_type not in d[sensor_id]:
                d[sensor_id][reading_type]=mag
                print(reading_type)
            else:
                d[sensor_id][reading_type] = np.vstack((d[sensor_id][reading_type], mag))
        for sensor in d:
            for axis in d[sensor]:
                classifier_array=d[sensor][axis]
                print(classifier_array)
                print(classifier_array.shape)
                print(sensor + "-" + axis)
                clf_axis=svm.OneClassSVM(gamma=0.001)
                clf_axis.fit(classifier_array)
                print('classifier made')
                joblib.dump(clf_axis, sensor + "-" + axis + ".pkl")
                print("saved " + sensor + "-" + axis)


if(__name__ == "__main__"):
    main()
