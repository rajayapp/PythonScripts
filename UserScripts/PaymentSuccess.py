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
    collection = db.user
    collection2 = db.user_payment
    startDate = str(sys.argv[1])
    endDate = str(sys.argv[2])
    start_date=[]
    end_date=[]
    for element in startDate.split('/'):
        start_date.append(element)

    for element in endDate.split('/'):
        end_date.append(element)

    start_timestamp = datetime.datetime(int(start_date[2]), int(start_date[0]), int(start_date[1]), 0, 0,
                                        0).timestamp() * 1000
    end_timestamp = datetime.datetime(int(end_date[2]), int(end_date[0]), int(end_date[1]), 0, 0, 0).timestamp() * 1000

    with open('PaymentSuccess.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID','Login-ID', 'Type']

        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        PaidUser = [];
        paymentSuccess=[]
        loginid="NULL"
        list =[]

        user_list = []
        dummy = []
        for user in collection.find():
            user_list.append(str(user['_id']))
            loginid1 = "NULL"
            for key in user:
                if (key == "loginid"):
                    loginid1 = user["loginid"];

            dummy.append({'id': str(user['_id']), 'Login-ID': loginid1})

        for user_rec in collection2.find():
            if(user_rec['userid'] not in list and start_timestamp <= int(user_rec['createdate']) <= end_timestamp):
                list.append(str(user_rec['userid']))



        for user_rec in collection.find():
            for key in user_rec:
                if (key == "loginid"):
                    if (user_rec[key] == "૯૯૨૪૫૨૬૨૯૮"):
                        loginid = "NULL"
                    else:
                        loginid = user_rec["loginid"];

            for key1 in user_rec:
                if (key1 == 'fullyregistered'):
                    if (user_rec[key1] == 'true' or user_rec[key1] == 'false'):
                        for key2 in user_rec:
                            if (key2 == 'userType'):
                                if (user_rec[key2] == 'PREMIUM'):
                                    PaidUser.append(str(user_rec['_id']))



        for val in PaidUser:
            if(str(val) in list):
                paymentSuccess.append(val)
                for user_rec in dummy:
                    if (user_rec['id'] == str(val)):
                        print(user_rec['Login-ID'])
                        writer.writerow({'Mongo-ID': val, 'Login-ID': user_rec['Login-ID'], 'Type': "Payment Success"})
                print(".................",val)


        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of Payment Success', 'COUNT': len(paymentSuccess)})


finally:
    print('done')