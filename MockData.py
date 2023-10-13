import pymongo
import sys
from multiprocessing import Pool
import random
import json
def insert_data():
    myclient = pymongo.MongoClient('mongodb://dbadmin:XXXXX@XXXXX.cpa1onesgnhw.docdb.cn-north-1.amazonaws.com.cn:27017/?tls=true&tlsCAFile=rds-combined-ca-cn-bundle.pem&replicaSet=rs0&readPreference=s
econdaryPreferred&retryWrites=false')
    for i in range(10000):
        data = []
        db = myclient["db2"]
        col = db.col_nike_test01
        for j in range(1000):
            enum_bool = ['true', 'false']
            enum_sexy = ['male', 'female']
            enum_city = ['Beijing','Shanghai','Guangzhou','Shenzhen','Hangzhou','Wuhan']
            enum_device = ['IOS','Android']
            random_id = random.randint(0,99999999)
            upmid = 'upmid_%s' % random_id
            mobile = '138%s' % random_id
            email = '%s@nike.com' % random_id
            nuid = 'nuid_%s' % random_id
            smsConsent = random.choice(enum_bool)
            emailConsent = random.choice(enum_bool)
            sexual = random.choice(enum_sexy)
            city = random.choice(enum_city)
            device = random.choice(enum_device)
            insertdata = '''{
            "journeyId" : 1,
            "upmid": "%s",
            "mobile": "%s",
            "email": "%s",
            "nuid": "%s",
            "smsConsent": "%s",
            "emailConsent": "%s",
            "nextStepId": 1,
            "traits": [
              {"tag": "sexual", "value": "%s"},
              {"tag": "city", "value": "%s" },
              {"tag": "device", "value": "%s"}
            ]
        }
         ''' % (upmid,mobile,email,nuid,smsConsent,emailConsent,sexual,city,device)
            json_insertdata = json.loads(insertdata)
            data.append(json_insertdata)
        col.insert_many(data)
        
if __name__ == '__main__':
    p = Pool()
    for i in range(5):
        p.apply(func=insert_data, args=())
    p.close()
    p.join()