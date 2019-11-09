from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import sys
import csv
try:
    conn2 = MongoClient()
    conn = MongoClient('52.89.157.209:27017',username = 'fintappM1',
    password = 'fintapp123',

    authSource = 'fintappv4dev',

    authMechanism = 'SCRAM-SHA-1')

    db = conn.fintappv4dev
    collection = db.user
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


    with open('FreeUser.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID', 'Type']
        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()



        GuestUser = [];
        loginid="NULL"
        count=0


        for user_rec in collection.find():
            flag=0
            flag_createdate=0
            for key in user_rec:
                if (key == "myStockBroker"):
                    flag = 1;
                if (key == "loginid"):
                    loginid = user_rec["loginid"];

                if (key == "createdate"):
                    flag_createdate = 1

            if (flag_createdate == 1 and start_timestamp <= int(user_rec['createdate']) <= end_timestamp):
                print(count)
                count=count+1
                for key in user_rec:
                    if(key=='fullyregistered'):
                        flag=1
                        if(user_rec[key]=='false'):
                            GuestUser.append(user_rec)
                            writer.writerow({'Mongo-ID': user_rec['_id'], 'LoginID': loginid, 'Type': "Free"})

                if (flag != 1):
                    GuestUser.append(user_rec)
                    writer.writerow({'Mongo-ID': user_rec['_id'], 'LoginID': loginid,'Type':"Free"})

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of Free Users', 'COUNT': len(GuestUser)})


        print("TOTAL FREE USERS",len(GuestUser))
finally:
    print('done')