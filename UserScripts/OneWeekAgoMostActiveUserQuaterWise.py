from pymongo import MongoClient
from _datetime import datetime ,timedelta
import csv
import sys
from bson.objectid import ObjectId

try:
    conn = MongoClient('52.89.157.209:27017', username='fintappM1',
                       password='fintapp123',

                       authSource='fintappv4dev',

                       authMechanism='SCRAM-SHA-1')

    conn2=MongoClient()

    db = conn.fintappv4dev
    db2 = conn2.fintappv4dev
    collection1 = db.user
    collection2 = db2.API_log_data
    map={}
    one_month_ago_activeuserlist=[]

    current_timestamp=datetime.now().timestamp()*1000
    one_month_ago_timestamp = (datetime.now() - timedelta(days=30)).timestamp()*1000

    jan_2017 = datetime(2017, 1, 1).timestamp() * 1000
    apr_2017 = datetime(2017, 4, 30).timestamp() * 1000

    jan_apr_2017_users = {}

    for data in collection2.find():
        val_UserID = ObjectId(data['UserID'])
        val_timestamp = data['timestamp']
        val_key  = map.get(val_UserID)
        if(val_key==None):
            map[val_UserID]=val_timestamp
        if(val_key!=None and val_timestamp > val_key):
            map[val_UserID] = val_timestamp





    for k,v in map.items():
        if (one_month_ago_timestamp <= v <= current_timestamp):
            one_month_ago_activeuserlist.append(k)


    #Some of don't have createdate field so we ignore that record#


    for user in collection1.find():
        val_id=user.get('_id')
        val_createdate=user.get('createdate')
        val_name=user.get('name')
        val_fname=user.get('fname')
        val_email=user.get('email')
        val_phoneno=user.get('phoneno')
        val_portfolioCount=user.get('portfolioCount')
        val_dematStatus=user.get('dematStatus')
        val_myAddress=user.get('myAddress')
        val_education=user.get('education')
        val_monthlyIncome=user.get('monthlyIncome')
        val_version=user.get('androidAppVersion')
        val_fullyregistered=user.get('fullyregistered')
        val_userType=user.get('userType')

        if(val_name==None and val_fname!=None):
            val_name=val_fname


        #Set user type
        if((val_fullyregistered!=None and val_fullyregistered=='true') or (val_userType!=None and (val_userType=='BASIC' or val_userType=='FREE'))):
            val_userType="BASIC"

        if ((val_fullyregistered != None and val_fullyregistered == 'false') or (
                val_userType != None and val_userType == 'FREE')):
            val_userType = "FREE"

        if ((val_fullyregistered != None and val_fullyregistered == 'true') and (
                val_userType != None and val_userType == 'PREMIUM')):
            val_userType = "PREMIUM"

        if (val_createdate != None and jan_2017 < float(
                val_createdate) < apr_2017 and val_id in one_month_ago_activeuserlist):
            jan_apr_2017_users[ObjectId(val_id)] = {'name': val_name, 'createdate': val_createdate, 'email': val_email,
                                                    'phoneno': val_phoneno, 'portfolioCount': val_portfolioCount,
                                                    'dematStatus': val_dematStatus
                , 'myAddress': val_myAddress, 'education': val_education, 'monthlyIncome': val_monthlyIncome,
                                                    'version': val_version, 'fullyregistered': val_fullyregistered
                , 'val_userType': val_userType}

    with open('Sep_Dec_2017.csv', mode='w') as csv_file:
        fieldnames = ['Mongo-ID', 'Name', 'PhoneNo', 'Email', 'Version', 'User Type']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for key ,val in jan_apr_2017_users.items():

            writer.writerow({'Mongo-ID': key, 'Name': val['name'], 'PhoneNo': val['phoneno'],
             'Email': val['email'],'Version': val['version'],'User Type': val['val_userType']
             })

            print("jan_apr_2017_users" , key ,val)

    print("jan_apr_2017_users",len(jan_apr_2017_users.items()))
    print("one_month_ago_timestamp", len(one_month_ago_activeuserlist))



finally:
    print('done')
