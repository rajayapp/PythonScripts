from pymongo import MongoClient
import csv
try:
    conn = MongoClient()
    print("Connected successfully!!!")
    db = conn.Fintapp
    collection = db.fintapp_user

    with open('DematNonDematHolder.csv', mode='w',encoding="utf-8" , newline='') as csv_file:
        fieldnames = ['Mongo-ID', 'LoginID', 'StockBroker']
        fieldnames2 = ['DematHolder', 'NonDematHolder']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        demat_user_count = []
        non_demat_user_count = []
        for user in collection.find():
            flag=0;
            demat_user = []
            non_demat_user = []
            loginid="NULL"

            for key in user:

                if (key == "myStockBroker"):
                    flag = 1;

                if (key == "loginid"):
                    loginid = user["loginid"];

                    # print(user)



            for key in user:

                if(key=='myStockBroker' and flag==1):
                    if (user[key] == "No Demat Account"):

                        non_demat_user_count.append(user)
                        non_demat_user.append(user["_id"])
                        non_demat_user.append(loginid)
                        non_demat_user.append(user[key])
                        print(non_demat_user)
                        writer.writerow({'Mongo-ID': non_demat_user[0], 'LoginID': non_demat_user[1],
                                         'StockBroker': non_demat_user[2]})

                    else:
                        demat_user_count.append(user)
                        demat_user.append(user["_id"])
                        demat_user.append(loginid)
                        demat_user.append(user[key])
                        writer.writerow({'Mongo-ID': demat_user[0], 'LoginID': demat_user[1],
                                         'StockBroker': demat_user[2]})



            if(flag==0):
                non_demat_user_count.append(user)
                non_demat_user.append(user["_id"])
                non_demat_user.append(loginid)
                non_demat_user.append("NULL")
                writer.writerow({'Mongo-ID': non_demat_user[0], 'LoginID': non_demat_user[1], 'StockBroker': non_demat_user[2]})



        writer2 = csv.DictWriter(csv_file, fieldnames=fieldnames2)
        writer2.writeheader()
        print(len(demat_user_count))
        print(len(non_demat_user_count))
        writer2.writerow({'DematHolder': len(demat_user_count), 'NonDematHolder': len(non_demat_user_count) })




finally:
    print('done')

