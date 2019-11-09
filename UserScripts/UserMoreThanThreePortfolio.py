from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import csv

try:

    conn = MongoClient('52.89.157.209:27017', username='fintappM1',
                       password='fintapp123',

                       authSource='fintappv4dev',

                       authMechanism='SCRAM-SHA-1')

    db = conn.fintappv4dev
    collection1 = db.user
    collection2 = db.portfolio
    pcount = "NULL"
    user_list = []
    list=[]

    with open('UserMoreThanThreePortfolio.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'PortfolioCount']
        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)



        for user in collection1.find():
            list.append(str(user['_id']))


        for portfolio in collection2.aggregate([{"$group": {"_id": {"UserId": "$userid"},
                                                           "PortfolioId": {"$addToSet": "$_id"}}}]):
            print(str(portfolio['_id']['UserId']))
            if (str(portfolio['_id']['UserId']) in list and  len(portfolio['PortfolioId']) > 3):
                user_list.append(portfolio)
                writer.writerow({'Mongo-ID': portfolio['_id']['UserId'], 'PortfolioCount': len(portfolio['PortfolioId'])})
                print(portfolio['_id']['UserId'], len(portfolio['PortfolioId']))


        writer2.writeheader()
        writer2.writerow({'QUERY': 'Total Number of USers > 3 Portfolio', 'COUNT': len(user_list)})




finally:
    print("done")






