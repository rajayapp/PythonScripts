from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import csv
import sys
import time
try:
    conn2 = MongoClient()
    conn = MongoClient('52.89.157.209:27017',username = 'fintappM1',
    password = 'fintapp123',

    authSource = 'fintappv4dev',

    authMechanism = 'SCRAM-SHA-1')
    userlist=[]
    db = conn.fintappv4dev
    collection = db.user
    start_date = []
    end_date = []
    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])


    for element in startDate.split('/'):
        start_date.append(element)

    for element in endDate.split('/'):
        end_date.append(element)

    start_timestamp = datetime.datetime(int(start_date[2]), int(start_date[0]), int(start_date[1]), 0, 0,
                                        0).timestamp() * 1000
    end_timestamp = datetime.datetime(int(end_date[2]), int(end_date[0]), int(end_date[1]), 0, 0, 0).timestamp() * 1000

    with open('JoinedUserDateWise.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID' ,'UserType','Joining Date' ]
        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for user in collection.find():
            #print(user)
            type = user.get('userType')
            createdate = "NULL"
            for key in user:
                if(key=='createdate'):
                    createdate = user['createdate']


            if(createdate!="NULL"):
                #print(user['createdate'])
                if (start_timestamp <= int(user['createdate']) <= end_timestamp):
                    userlist.append(user)
                    epoch_date = time.strftime('%d-%m-%Y %H:%M:%S',  time.gmtime(float(user["createdate"])/1000))
                    print(epoch_date)
                    writer.writerow({'Mongo-ID': user["_id"], 'LoginID': user['loginid'],'UserType':type,'Joining Date': epoch_date})

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of Users', 'COUNT': len(userlist)})






finally:
    print("done")