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

    db = conn.fintappv4dev
    db2 = conn2.fintappv4dev
    collection1 = db.user
    collection2 = db2.log_data
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
    global_var=[]
    with open('VersionWiseFilterResult.csv', mode='w') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID' ,'Version' ]
        fieldnames2 = ['Version', 'Count']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()

        for user in  collection1.aggregate([{"$group": {"_id": {"UserId" :"$_id"},"LoginID": {"$addToSet": "$loginid"},"AppVersion": {"$addToSet": "$androidAppVersion"},
                                                        "createdate": {"$addToSet": "$createdate"}}} ]):

            if(len(user['createdate'])>0 and start_timestamp <= int(user['createdate'][0]) <= end_timestamp):


                list=[]


                #print(user['_id']["UserId"])
                list.append(user["_id"]["UserId"])

                if (len(user["LoginID"])==1):
                    #print(user["LoginID"][0])
                    list.append(user["LoginID"][0])

                else:
                    #print("null")
                    list.append("NULL")

                if (len(user["AppVersion"])==1):
                    #print(user["AppVersion"][0])
                    list.append(user["AppVersion"][0])
                else:
                    #print("null")
                    list.append("NULL")

                flag=0
                for val in global_var:
                    if(val['AppVersion']==user['AppVersion']):
                        val['count']=val['count']+1
                        flag=1

                if(flag==0):
                    global_var.append({'AppVersion':user['AppVersion'],'count':1})



                print(list)
                try:
                    writer.writerow({'Mongo-ID': list[0], 'LoginID': list[1] , 'Version': list[2]})

                except KeyError as e:
                    writer.writerow({'Mongo-ID': list[0], 'LoginID': "NULL", 'Version': list[2]})

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()

        print(global_var)
        for val in global_var:
            if(len(val["AppVersion"])>0):
                writer2.writerow({'Version': val["AppVersion"][0], 'Count': val["count"]})
            if (len(val["AppVersion"]) == 0):
                writer2.writerow({'Version': 'NULL', 'Count': val["count"]})





finally:
    print('done')

