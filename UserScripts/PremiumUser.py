from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import csv
import sys
try:
    conn2 = MongoClient()
    conn = MongoClient('52.89.157.209:27017',username = 'fintappM1',
    password = 'fintapp123',

    authSource = 'fintappv4dev',

    authMechanism = 'SCRAM-SHA-1')

    start_date =[]
    end_date =[]
    db = conn.fintappv4dev
    collection = db.user

    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])



    for element in startDate.split('/'):
        start_date.append(element)

    for element in endDate.split('/'):
        end_date.append(element)

    start_timestamp = datetime.datetime(int(start_date[2]), int(start_date[0]), int(start_date[1]), 0, 0,
                                        0).timestamp() * 1000
    end_timestamp = datetime.datetime(int(end_date[2]), int(end_date[0]), int(end_date[1]), 0, 0, 0).timestamp() * 1000

    with open('PremiumUser.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID', 'Type']

        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        PaidUser = [];
        loginid="NULL"



        for user_rec in collection.find():

            flag=0
            for key in user_rec:
                if (key == "loginid"):
                    if (user_rec[key] == "૯૯૨૪૫૨૬૨૯૮"):
                        loginid = "NULL"
                    else:
                        loginid = user_rec["loginid"];

                if(key == "createdate"):
                    flag=1



            if (flag==1 and start_timestamp <= int(user_rec['createdate']) <= end_timestamp):
                for key1 in user_rec:
                    if (key1 == 'fullyregistered'):
                        if (user_rec[key1] == 'true' or user_rec[key1] == 'false'):
                            for key2 in user_rec:
                                if (key2 == 'userType'):
                                    if (user_rec[key2] == 'PREMIUM'):
                                        PaidUser.append(user_rec)
                                        writer.writerow({'Mongo-ID': user_rec['_id'], 'LoginID': loginid,'Type':"Premium"})

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of Premium Users', 'COUNT': len(PaidUser)})


        print("TOTAL FREE USERS",len(PaidUser))
finally:
    print('done')