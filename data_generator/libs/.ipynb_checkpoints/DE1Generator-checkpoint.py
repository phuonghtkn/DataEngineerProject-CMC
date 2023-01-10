import sys
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType
import random
import faker
from datetime import datetime
import re


#config constant
CONFIG_FILE_PATH_PREFIX = '../cfg/'
CONFIG_TMP_FILE_PATH_PREFIX = '../cfg/tmp/'
CONFIG_BACKUP_FILE_PATH_PREFIX = '../cfg/backup/'

OUTPUT_FILE_PATH_PREFIX = '../output/'
OUTPUT_TMP_FILE_PATH_PREFIX = '../output/tmp/'

class AccountObject:
    AcountTypeDim = {
        1: "credit",
        2: "debit",
        3: "deposit",
        4: "saving"
    }
    StatusDim = {
        1: "Active",
        2: "Inactive",
        3: "Deleted",
        4: "OnewayInactive"
    }
    customerType = {
        1: "Company",
        2: "Invidual",
        3: "LinkedCompanyUser"
    }
    fake = faker.Faker()

    def __init__(self, accountrecordid, defaultaccountrecordid, accountid, defaultaccountid, defaultcustomerid, customerid):
        self.accountrecordid = accountrecordid
        #idConfig["accountrecordid"] += 1
        self.accountid = accountid
        #idConfig["accountid"] += 1
        __accounttype = random.choice(list(AccountObject.AcountTypeDim.keys()))
        self.accounttype = AccountObject.AcountTypeDim[__accounttype]
        self.customerType = AccountObject.customerType[2]
        self.customerid = AccountObject.fake.random_int(min=defaultcustomerid, max=customerid)
        self.balance = random.randint(1, 10000)
        self.opendate = AccountObject.fake.date_this_month().strftime("%Y-%m-%d")
        self.closedate = ""
        self.duration = ""
        self.initialamount = ""
        self.remainamount = ""
        self.totalamount = ""
        self.status = AccountObject.StatusDim[1]
        self.loanDate = ""
        self.loanTypeID = ""
        self.referenceinterestrate = ""
        self.interestID = ""
        self.cardinfo = AccountObject.fake.credit_card_full()

    def toList(self):
        return [Row(
            self.accountrecordid,
            self.accountid,
            self.accounttype,
            self.customerType,
            self.customerid,
            self.balance,
            self.opendate,
            self.closedate,
            self.duration,
            self.initialamount,
            self.remainamount,
            self.totalamount,
            self.status,
            self.loanDate,
            self.loanTypeID,
            self.referenceinterestrate,
            self.interestID,
            self.cardinfo
            )]
    
class AccountObjects:
    def __init__(self):
        self.StopSparked = False
        self.FactAccount = []
        AccountObjects.StartSpark()
        
        self.idConfig = {
            "accountrecordid": 10000000,
            "defaultaccountrecordid": 10000000,
            "accountid": 10000000,
            "defaultaccountid": 10000000,
            "defaultcustomerid": 10000000,
            "customerid": 10000000,
        }
        for file in self.idConfig.keys():
            file_exist = os.path.exists(CONFIG_FILE_PATH_PREFIX+file)
            if (file_exist):
                realFile = open(CONFIG_FILE_PATH_PREFIX+file,"r")
                self.idConfig[file] = int(realFile.readlines()[0])
                realFile.close()
            else:
                if (file not in ["accountid", "defaultaccountid", "accountrecordid", "defaultaccountrecordid"]):
                    sys.exit('Please run CustomerFactGenerator first')
                    break
                realFile = open(CONFIG_FILE_PATH_PREFIX+file,"w+")
                realFile.seek(0)
                realFile.write(str(self.idConfig[file]))
                realFile.close()
    def __del__(self):
        AccountObjects.StopSpark()
    
    def StartSpark(self):
        if(self.StopSparked == True):
            self.spark = SparkSession \
                .builder \
                .appName("Python Spark Customer Fact Generator") \
                .getOrCreate()
            self.StopSparked = False
        
    def StopSpark(self):
        if(self.StopSparked == False):
            self.spark.stop()
            self.StopSparked = True
        
    def create_data(NumbefOfRow):
        for x in range(NumbefOfRow):
            Account = AccountObject(self.idConfig["accountrecordid"], self.idConfig["defaultaccountrecordid"], self.idConfig["accountid"], self.idConfig["defaultaccountid"], self.idConfig["defaultcustomerid"], self.idConfig["customerid"])
            self.idConfig["accountrecordid"] += 1
            self.idConfig["accountid"] += 1
            self.FactAccount = self.FactAccount + Account.toList()
        FactAccountColumns = StructType([
            StructField("accountrecordid", StringType(), True),
            StructField("accountid", StringType(), True),
            StructField("accounttype", StringType(), True),
            StructField("customerType", StringType(), True),
            StructField("customerid", StringType(), True),
            StructField("balance", StringType(), True),
            StructField("opendate", StringType(), True),
            StructField("closedate", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("initialamount", StringType(), True),
            StructField("remainamount", StringType(), True),
            StructField("totalamount", StringType(), True),
            StructField("status", StringType(), True),
            StructField("loanDate", StringType(), True),
            StructField("loanTypeID", StringType(), True),
            StructField("referenceinterestrate", StringType(), True),
            StructField("interestID", StringType(), True),
            StructField("cardinfo", StringType(), True),
            ])
        now = datetime.now()
        self.fileName = 'FactAccount ' + now.strftime("%Y-%m-%d %H-%M-%S-%f")
        self.MainDf = spark.createDataFrame(data=FactAccount, schema = FactAccountColumns)
        return [self.fileName, self.MainDf]
    def to_File():
        self.MainDf.write.mode('overwrite').option("header","true").csv(OUTPUT_TMP_FILE_PATH_PREFIX + fileName)
        for file in self.idConfig.keys():
            realFile = open(CONFIG_TMP_FILE_PATH_PREFIX+file,"w+")
            realFile.seek(0)
            realFile.write(str(self.idConfig[file]))
            realFile.close()