from pymongo import MongoClient
import datetime
import csv
import sys
from bson.objectid import ObjectId

try:

    conn = MongoClient('52.89.157.209:27017', username='fintappM1',
                       password='fintapp123',

                       authSource='fintappv4dev',

                       authMechanism='SCRAM-SHA-1')

    db = conn.fintappv4dev
    collection1 = db.user
    collection2 = db.portfolio
    BasicUser = []
    count=0
    final_amount=0
    print("connected")
    b_user=[]

    for user_rec in collection1.find():
        flag_createdate = 0
        for key in user_rec:
            if (key == "loginid"):
                loginid = user_rec["loginid"];

            if (key == "createdate"):
                flag_createdate = 1

        if (flag_createdate == 1):
            for key1 in user_rec:
                if (key1 == 'fullyregistered'):
                    if (user_rec[key1] == 'true'):
                        set_flag = 0
                        for key2 in user_rec:
                            if (key2 == 'userType'):
                                set_flag = 1
                                if (user_rec[key2] == 'BASIC' or user_rec[key2] == 'FREE'):
                                    BasicUser.append(user_rec['_id'])
                        if (set_flag != 1):
                            BasicUser.append(user_rec['_id'])

    for item in collection2.find():
        val_1 = item.get('name')
        val_2=  item.get('userid')
        val_3 = item.get('totalamount')

        if(val_1!=None and ObjectId(val_2) in BasicUser and val_3!=None and val_3!=0):
            count = count + 1
            final_amount =final_amount+ float(val_3)

        if(val_2 not in b_user and ObjectId(val_2) in BasicUser):
            b_user.append(val_2)


            print(count)

    print("Total Basic users who created Portfolio", len(b_user))
    print("Total Number of Portfolio",count)
    print("Total Amount",final_amount)
    print("Total Basic Users",len(BasicUser))

finally:
    print('done')