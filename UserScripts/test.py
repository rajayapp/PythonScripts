from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import csv
import sys
try:
    conn2 = MongoClient()
    conn = MongoClient('52.89.157.209:27017', username='fintappM1',
                       password='fintapp123',

                       authSource='fintappv4dev',

                       authMechanism='SCRAM-SHA-1')

    db = conn.fintappv4dev
    db2 = conn2.fintappv4dev
    collection = db.user
    collection2 = db2.log_data
    start_date = []
    end_date = []
    ActiveUserList = []
    InActiveUserList = []

    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])

    for element in startDate.split('/'):
        start_date.append(element)

    for element in endDate.split('/'):
        end_date.append(element)

    start_timestamp = datetime.datetime(int(start_date[2]), int(start_date[0]), int(start_date[1]), 0, 0,
                                        0).timestamp() * 1000
    end_timestamp = datetime.datetime(int(end_date[2]), int(end_date[0]), int(end_date[1]), 0, 0, 0).timestamp() * 1000

    print("Starting date ", start_timestamp)
    print("Ending date ", end_timestamp)

    with open('VersionWiseActiveUser3.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID', 'AndroidAppVersion', 'Category']
        fieldnames2 = ['ActiveUserList', 'InActiveUserList']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for user in collection2.aggregate([{'$group': {"_id": {'UserId': "$user_id"},
                                                       "LoginId": {"$addToSet": "$login_str"},
                                                       "timestamp": {"$addToSet": "$timestamp"},
                                                       "androidAppVersion": {"$addToSet": "$androidAppVersion"}}}]):
            list = [];
            list.append(max(user["timestamp"]))  # 0
            list.append(user["_id"]["UserId"])  # 1

            try:
                list.append(user["LoginId"][0])  # 2
            except:
                list.append("NULL")  # 2

            list.append(user["androidAppVersion"][0])  # 3
            if (start_timestamp <= max(user["timestamp"]) <= end_timestamp):
                list.append("ActiveUser")  # 4
                ActiveUserList.append(user)
                writer.writerow({'Mongo-ID': list[1], 'LoginID': list[2], 'AndroidAppVersion': list[3],
                                 'Category': list[4]})


            else:
                list.append("InActiveUser")  # 4
                InActiveUserList.append(user)

                writer.writerow(
                    {'Mongo-ID': list[1], 'LoginID': list[2], 'AndroidAppVersion': list[3], 'Category': list[4]})

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'ActiveUserList': len(ActiveUserList), 'InActiveUserList': len(InActiveUserList)})
        print(len(ActiveUserList))
        print(len(InActiveUserList))


finally:
    print('done')

