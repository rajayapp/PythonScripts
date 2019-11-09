from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import csv
try:

    conn = MongoClient('52.89.157.209:27017',username = 'fintappM1',
    password = 'fintapp123',

    authSource = 'fintappv4dev',

    authMechanism = 'SCRAM-SHA-1')

    db = conn.fintappv4dev
    collection = db.user

    with open('KYCFailure.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID','Login-ID', 'Type']

        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()


        fail_user = []

        for user in collection.find():
            kyc_flag = "NULL"
            panno = "NULL"
            loginid = "NULL"
            for key in user:
                if (key == "loginid"):
                    if (user[key] == "૯૯૨૪૫૨૬૨૯૮"):
                        loginid = "NULL"
                    else:
                        loginid = user["loginid"];

            for key in user:
                if(key=='panNo'):
                    panno=user[key]
                if(key=='kycStatus' and user[key]=='false'):
                    kyc_flag=user[key]

            if(panno!="NULL" and kyc_flag=="false"):
                fail_user.append(user["_id"])
                writer.writerow({'Mongo-ID': user["_id"],'Login-ID':loginid, 'Type': "KYC Failure"})

        print(len(fail_user))
        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of KYC Failure', 'COUNT': len(fail_user)})


finally:
    print("done")