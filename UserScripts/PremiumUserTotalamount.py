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
    collection_2 = db.portfolio
    totalamount=0
    uid = {}
    uid_name = {}
    port_count = 0
    with open('PremiumPortfolioAmount.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Name', 'User-ID','Portfolio-ID' , 'Amount']
        fieldnames2 = ['QUERY', 'AMOUNT']

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for user in collection.find():
            val = user.get('userType')
            val_2 = user.get('name')
            if(val=='PREMIUM'):
                uid[str(user['_id'])] = str(user['_id'])
                uid_name[str(user['_id'])]= val_2

        for port in collection_2.find():
            val = port['userid']
            val_2 = uid.get(val)
            val_3 = port.get('totalamount')
            val_4 = uid_name.get(val)
            val_5 = port.get('name')
            if(val!=None and val_2!=None and val_3!=None and val_5!=None):
                port_count=port_count+1
                writer.writerow({'Name': val_4, 'User-ID':val,'Portfolio-ID':port['_id'] ,'Amount':val_3})
                print(port['_id'])
                print(val_3)
                totalamount = totalamount+float(val_3)

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Premium Users', 'AMOUNT': len(uid)})
        writer2.writerow({'QUERY': 'Total Premium Users Portfolio count', 'AMOUNT':port_count})
        writer2.writerow({'QUERY': 'Total Amount of Portfolio', 'AMOUNT': totalamount})



    print(totalamount)
    print(len(uid))
    print(port_count)
finally:
    print('done')