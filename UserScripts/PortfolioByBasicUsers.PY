from pymongo import MongoClient
import datetime
import csv
import sys
try:

    conn = MongoClient('52.89.157.209:27017', username='fintappM1',
                       password='fintapp123',

                       authSource='fintappv4dev',

                       authMechanism='SCRAM-SHA-1')

    db = conn.fintappv4dev
    collection1 = db.user
    collection2 = db.portfolio
    start_date = []
    end_date = []
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

    BasicUser=[]
    print(start_timestamp)
    print(end_timestamp)
    with open('PortfolioByBasicUsers.csv', mode='w') as csv_file:
        fieldnames = ['Mongo-ID','Login-ID','PortfolioCount']
        fieldnames2 = ['QUERY', 'COUNT']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)

        for user_rec in collection1.find():
            loginid = "NULL"
            for key in user_rec:
                if (key == "loginid"):
                   loginid = user_rec["loginid"];


            for key1 in user_rec:
                set_flag = 0

                if (key1 == 'fullyregistered'):
                    if (user_rec[key1] == 'true'):
                        for key2 in user_rec:
                            if (key2 == 'userType'):
                                if (user_rec[key2] == 'BASIC' or user_rec[key2] == 'FREE'):
                                    set_flag = 1
                                    BasicUser.append({'userid':str(user_rec['_id']),'loginid':loginid})
                        if (set_flag != 1):
                            #print(loginid)
                            BasicUser.append({'userid':str(user_rec['_id']),'loginid':loginid})

        count1=0
        count2=0
        print(BasicUser)


        for portfolio in collection2.aggregate([{"$match":{"createdate":{"$gt":str(start_timestamp) , "$lt":str(end_timestamp)}}},{"$group": {"_id": {"UserId": "$userid"},
                                                           "PortfolioId": {"$addToSet": "$_id"}}}]):
            for val in BasicUser:
               if(str(val['userid'])==portfolio['_id']['UserId']):
                    count1 = count1+1
                    count2 =count2+len(portfolio['PortfolioId'])
                    writer.writerow({'Mongo-ID': portfolio['_id']['UserId'],'Login-ID': val['loginid'],'PortfolioCount': len(portfolio['PortfolioId'])})

        writer2.writeheader()
        print(count1)
        print(count2)
        writer2.writerow({'QUERY': 'Total Number Basic User who created portfolio ', 'COUNT': count1})
        writer2.writerow({'QUERY': 'Total Number created portfolio ', 'COUNT': count2})




finally:
    print("done")