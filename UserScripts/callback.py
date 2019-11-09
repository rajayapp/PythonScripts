from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import csv
import sys
try:

    conn = MongoClient('52.89.157.209:27017',username = 'dashboardU1',
    password = 'fintapp123',

    authSource = 'admin',

    authMechanism = 'SCRAM-SHA-1')
    db = conn.dashboard
    collection1 = db.feedback

    start_date=[]
    end_date=[]
    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])

    #startDate = input("Please Enter Start date Seperated by -")
    #endDate = input("Please Enter End date Seperated by - ")

    for element in startDate.split('/'):
        start_date.append(element)

    for element in endDate.split('/'):
        end_date.append(element)

    start_timestamp = datetime.datetime(int(start_date[2]), int(start_date[0]), int(start_date[1]), 0, 0,
                                        0).timestamp() * 1000
    end_timestamp = datetime.datetime(int(end_date[2]), int(end_date[0]), int(end_date[1]), 0, 0, 0).timestamp() * 1000

    with open('callback.csv', mode='w' , encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'Name','createdate','callbackdate']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()




        for feedback in collection1.find():

            val_1 = feedback.get('userid')
            val_2 = feedback.get('Name')
            val_3 = feedback.get('createtimestamp')
            val_4 = feedback.get('createdate')
            val_5 = feedback.get('callbacktimestamp')
            val_6 = feedback.get('callbackdate')
            val_7 = feedback.get('follow_up_type')
            val_8 = feedback.get('callback')
            if(start_timestamp <=float(val_5) <=end_timestamp and val_8 == 'YES'):
                writer.writerow({'Mongo-ID': val_1,
                                 'Name': val_2,
                                 'createdate': val_4,
                                 'callbackdate': val_6,
                                 'follow type':val_7

                                 })




finally:
    print('done')