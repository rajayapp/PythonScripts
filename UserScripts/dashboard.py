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
    collection = db.user
    collection_1=db.portfolio
    collection_2 = db.portfolioStatus
    basic_user_count = 0
    free_user_count = 0
    premium_user_count=0
    lastMonth_basic_user_count = 0
    lastMonth_free_user_count = 0
    lastMonth_premium_user_count=0
    lastweek_basic_user_count = 0
    lastweek_free_user_count = 0
    lastweek_premium_user_count = 0
    lastday_basic_user_count = 0
    lastday_free_user_count = 0
    lastday_premium_user_count = 0
    totalamount=0
    portfoliocount=0
    savedportfoliocount=0
    total_users = 0
    kyc_Success_count=0
    kyc_failure_count=0
    latest_version_users =0
    beginnerPortfolioCount=0
    positiveportfoliocount=0
    positiveportfolioPercentage=0
    premium_users = {}
    one_day_ago_timestamp = (datetime.now() - timedelta(days=1)).timestamp()*1000
    one_week_ago_timestamp = (datetime.now() - timedelta(days=7)).timestamp()*1000
    one_month_ago_timestamp = (datetime.now() - timedelta(days=31)).timestamp()*1000
    current_timestamp = datetime.now()

    print(one_month_ago_timestamp)
    current_timestamp=datetime.now().timestamp()*1000

    for user in collection.find():
        val_id=user.get('_id')
        val_name=user.get('name')
        val_fullyregistered=user.get('fullyregistered')
        val_userType=user.get('userType')
        val_createdate=user.get('createdate')
        val_kyc_Success = user.get('kycStatus')
        val_pan = user.get('panNo')
        appversion = user.get('androidAppVersion')

        if (val_kyc_Success == 'true'):
            kyc_Success_count = kyc_Success_count + 1

        if (val_pan != None and val_kyc_Success == 'false'):
            kyc_failure_count = kyc_failure_count + 1

        if (appversion == '2.1.5' or appversion == '2.1.7'):
            latest_version_users = latest_version_users + 1

        if(val_userType==None):
            if(val_fullyregistered!=None and val_fullyregistered=='true'):
                basic_user_count=basic_user_count+1

            if (val_fullyregistered == None or val_fullyregistered == 'false'):
                free_user_count = free_user_count + 1

        if(val_userType!=None and val_userType=='FREE'):
            free_user_count = free_user_count + 1

        if (val_userType != None and val_userType == 'BASIC'):
            basic_user_count = basic_user_count + 1


        if (val_userType != None and val_userType == 'PREMIUM'):
            premium_user_count = premium_user_count + 1
            premium_users[str(val_id)]=val_name

        if( val_createdate!=None):
            if (val_userType == None ):
                if (val_fullyregistered != None and val_fullyregistered == 'true' and  one_day_ago_timestamp < float(val_createdate)):
                    lastday_basic_user_count = lastday_basic_user_count + 1

                if (val_fullyregistered != None and val_fullyregistered == 'false' and  float(one_day_ago_timestamp) <= float(val_createdate)):
                    lastday_free_user_count = lastday_free_user_count + 1

            if (val_userType != None and val_userType == 'FREE' and  float(one_day_ago_timestamp) <= float(val_createdate)):
                lastday_free_user_count = lastday_free_user_count + 1

            if (val_userType != None and val_userType == 'BASIC' and  float(one_day_ago_timestamp) <= float(val_createdate)):
                lastday_basic_user_count = lastday_basic_user_count + 1

            if (val_userType != None and val_userType == 'PREMIUM' and  float(one_day_ago_timestamp) <= float(val_createdate)):
                lastday_premium_user_count = lastday_premium_user_count + 1

        if (val_createdate != None):
            if (val_userType == None):
                if (val_fullyregistered != None and val_fullyregistered == 'true' and one_week_ago_timestamp < float(
                        val_createdate)):
                    lastweek_basic_user_count = lastweek_basic_user_count + 1

                if (val_fullyregistered != None and val_fullyregistered == 'false' and float(
                        one_week_ago_timestamp) <= float(val_createdate)):
                    lastweek_free_user_count = lastweek_free_user_count + 1

            if (val_userType != None and val_userType == 'FREE' and float(one_week_ago_timestamp) <= float(
                    val_createdate)):
                lastweek_free_user_count = lastweek_free_user_count + 1

            if (val_userType != None and val_userType == 'BASIC' and float(one_week_ago_timestamp) <= float(
                    val_createdate)):
                lastweek_basic_user_count = lastweek_basic_user_count + 1

            if (val_userType != None and val_userType == 'PREMIUM' and float(one_week_ago_timestamp) <= float(
                    val_createdate)):
                lastweek_premium_user_count = lastweek_premium_user_count + 1

        if (val_createdate != None):
            if (val_userType == None):
                if (val_fullyregistered != None and val_fullyregistered == 'true' and one_month_ago_timestamp < float(
                        val_createdate)):
                    lastMonth_basic_user_count = lastMonth_basic_user_count + 1

                if (val_fullyregistered != None and val_fullyregistered == 'false' and float(
                        one_month_ago_timestamp) <= float(val_createdate)):
                    lastMonth_free_user_count = lastMonth_free_user_count + 1

            if (val_userType != None and val_userType == 'FREE' and float(one_month_ago_timestamp) <= float(
                    val_createdate)):
                lastMonth_free_user_count = lastMonth_free_user_count + 1

            if (val_userType != None and val_userType == 'BASIC' and float(one_month_ago_timestamp) <= float(
                    val_createdate)):
                lastMonth_basic_user_count = lastMonth_basic_user_count + 1

            if (val_userType != None and val_userType == 'PREMIUM' and float(one_month_ago_timestamp) <= float(
                    val_createdate)):
                lastMonth_premium_user_count = lastMonth_premium_user_count + 1

    for item in collection_1.find():
        portfoliocount=portfoliocount+1

        val_1 = item.get('totalamount')
        val_2 = item.get('name')
        val_3 = item.get('mode')

        if(val_1!=None):
            totalamount = totalamount+float(val_1)

        if(val_2!=None):
            savedportfoliocount=savedportfoliocount+1

        if(val_3=='auto'):
            beginnerPortfolioCount=beginnerPortfolioCount+1

    for item in collection_2.find():
        gainPer = item['gainPer']
        if(gainPer>0):
            positiveportfoliocount = positiveportfoliocount+1

    positiveportfolioPercentage = (positiveportfoliocount/portfoliocount)*100

    with open('DD.csv', mode='w') as csv_file:
        fieldnames = ['QUERY', 'VALUE']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        writer.writerow({'QUERY': 'FREE USERS','VALUE':free_user_count})
        writer.writerow({'QUERY': 'BASIC USERS','VALUE':basic_user_count})
        writer.writerow({'QUERY': 'PREMIUM USERS','VALUE':premium_user_count})
        writer.writerow({'QUERY': 'LAST MONTH FREE USERS','VALUE':lastMonth_free_user_count})
        writer.writerow({'QUERY': 'LAST MONTH BASIC USERS','VALUE':lastMonth_basic_user_count})
        writer.writerow({'QUERY': 'LAST MONTH PREMIUM USERS','VALUE':lastMonth_premium_user_count})
        writer.writerow({'QUERY': 'TOTAL USERS','VALUE':free_user_count + basic_user_count + premium_user_count})
        writer.writerow({'QUERY': 'KYC SUCCESS USER COUNT','VALUE': kyc_Success_count})
        writer.writerow({'QUERY': 'KYC FAILURE USER COUNT','VALUE':kyc_failure_count})
        writer.writerow({'QUERY': 'LATEST VERSION USERS','VALUE':latest_version_users})
        writer.writerow({'QUERY': 'TOTAL PORTFOLIO COUNT','VALUE':portfoliocount})
        writer.writerow({'QUERY': 'TOTAL SAVE PORTFOLIO','VALUE':savedportfoliocount})
        writer.writerow({'QUERY': 'TOTAL BEGINNER PORTFOLIO','VALUE':beginnerPortfolioCount})
        writer.writerow({'QUERY': 'TOTAL POSITIVE PORTFOLIO','VALUE':positiveportfoliocount})
        writer.writerow({'QUERY': 'TOTAL PERCENTAGE OF POSITIVE PORTFOLIO','VALUE':str(positiveportfolioPercentage)+'%'})
        writer.writerow({'QUERY': 'TOTAL PERCENTAGE OF Negative PORTFOLIO','VALUE':str(100-float(positiveportfolioPercentage))+'%'})
        writer.writerow({'QUERY': 'LAST WEEK FREE USERS','VALUE':lastweek_free_user_count})
        writer.writerow({'QUERY': 'LAST WEEK BASIC USERS','VALUE':lastweek_basic_user_count})
        writer.writerow({'QUERY': 'LAST WEEK PREMIUM USERS','VALUE':lastweek_premium_user_count})
        writer.writerow({'QUERY': 'LAST DAY FREE USERS','VALUE':lastday_free_user_count})
        writer.writerow({'QUERY': 'LAST DAY BASIC USERS','VALUE':lastday_basic_user_count})
        writer.writerow({'QUERY': 'LAST DAY PREMIUM USERS','VALUE':lastday_premium_user_count})
        writer.writerow({'QUERY': 'UPDATED DATE', 'VALUE': current_timestamp})
        writer.writerow({'QUERY': 'TOTAL AMOUNT OF ALL THE PORTFOLIO', 'VALUE': totalamount})


finally:
    print('done')