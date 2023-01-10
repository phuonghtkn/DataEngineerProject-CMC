import os
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
import random
import faker
from datetime import datetime
import shutil

PATH = os.path.realpath(os.path.dirname(__file__))

# config constant
CONFIG_FILE_PATH_PREFIX = PATH + "/../cfg/"
CONFIG_TMP_FILE_PATH_PREFIX = PATH + "/../cfg/tmp/"
CONFIG_BACKUP_FILE_PATH_PREFIX = PATH + "/../cfg/backup/"

OUTPUT_FILE_PATH_PREFIX = PATH + "/../output/"
OUTPUT_TMP_FILE_PATH_PREFIX = PATH + "/../output/tmp/"


class BankAccount:
    acount_type_dim = {1: "credit", 2: "debit", 3: "deposit", 4: "saving"}
    status_dim = {
        1: "Active",
        2: "Inactive",
        3: "Deleted",
        4: "OnewayInactive",
    }
    customer_type_dim = {1: "Company", 2: "Invidual", 3: "LinkedCompanyUser"}
    fake = faker.Faker()

    def __init__(
        self,
        accountrecordid,
        accountid,
        defaultcustomerid,
        customerid,
    ):
        self.accountrecordid = accountrecordid
        self.accountid = accountid
        __accounttype = random.choice(list(self.acount_type_dim.keys()))
        self.accounttype = self.acount_type_dim[__accounttype]
        self.customer_type_dim = self.customer_type_dim[2]
        self.customerid = self.fake.random_int(
            min=defaultcustomerid, max=customerid
        )
        self.balance = random.randint(1, 10000)
        self.opendate = self.fake.date_this_month().strftime("%Y-%m-%d")
        self.closedate = ""
        self.duration = ""
        self.initialamount = ""
        self.remainamount = ""
        self.totalamount = ""
        self.status = self.status_dim[1]
        self.loanDate = ""
        self.loanTypeID = ""
        self.referenceinterestrate = ""
        self.interestID = ""
        self.cardinfo = self.fake.credit_card_full()

    def toList(self):
        return [
            Row(
                self.accountrecordid,
                self.accountid,
                self.accounttype,
                self.customer_type_dim,
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
                self.cardinfo,
            )
        ]


class FakeAccountGenerator:
    def __init__(self):
        self.spark_stoped = False
        self.fact_account = []
        self.spark = SparkSession.builder.appName(
            "Python Spark Customer Fact Generator"
        ).getOrCreate()

        self.id_config = {
            "accountrecordid": 10000000,
            "defaultaccountrecordid": 10000000,
            "accountid": 10000000,
            "defaultaccountid": 10000000,
            "defaultcustomerid": 10000000,
            "customerid": 10000000,
        }
        for file in self.id_config.keys():
            file_exist = os.path.exists(CONFIG_FILE_PATH_PREFIX + file)
            if file_exist:
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "r")
                self.id_config[file] = int(realFile.readlines()[0])
                realFile.close()
            else:
                if file not in [
                    "accountid",
                    "defaultaccountid",
                    "accountrecordid",
                    "defaultaccountrecordid",
                ]:
                    sys.exit("Please run CustomerFactGenerator first")
                    break
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "w+")
                realFile.seek(0)
                realFile.write(str(self.id_config[file]))
                realFile.close()

    def __del__(self):
        self.stop_spark()

    def start_spark(self):
        if self.spark_stoped:
            self.spark = SparkSession.builder.appName(
                "Python Spark Customer Fact Generator"
            ).getOrCreate()
            self.spark_stoped = False

    def stop_spark(self):
        if not self.spark_stoped:
            self.spark.stop()
            self.spark_stoped = True

    def create_data(self, NumbefOfRow):
        for _ in range(NumbefOfRow):
            bank_account_record = BankAccount(
                self.id_config["accountrecordid"],
                self.id_config["accountid"],
                self.id_config["defaultcustomerid"],
                self.id_config["customerid"],
            )
            self.id_config["accountrecordid"] += 1
            self.id_config["accountid"] += 1
            self.fact_account = (
                self.fact_account + bank_account_record.toList()
            )
        account_column_layout = StructType(
            [
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
            ]
        )
        now = datetime.now()
        self.file_name = "FactAccount " + now.strftime("%Y-%m-%d %H-%M-%S-%f")
        self.fact_bank_acc_df = self.spark.createDataFrame(
            data=self.fact_account, schema=account_column_layout
        )
        return [self.file_name, self.fact_bank_acc_df]

    def to_file(self):
        print(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        self.fact_bank_acc_df.write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        for file in self.id_config.keys():
            realFile = open(CONFIG_TMP_FILE_PATH_PREFIX + file, "w+")
            realFile.seek(0)
            realFile.write(str(self.id_config[file]))
            realFile.close()
        for file in self.id_config.keys():
            shutil.move(
                CONFIG_FILE_PATH_PREFIX + file,
                CONFIG_BACKUP_FILE_PATH_PREFIX + file,
            )
            shutil.move(
                CONFIG_TMP_FILE_PATH_PREFIX + file,
                CONFIG_FILE_PATH_PREFIX + file,
            )
        shutil.move(
            OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name,
            OUTPUT_FILE_PATH_PREFIX + self.file_name,
        )


class BankCustomer:
    customer_type = {
        1: "InvidiualUser",
        2: "LinkedCompanyUser",
        3: "LinkedInvidiualUser",
    }
    gender = {1: "male", 2: "female"}
    personal_identityType = {
        1: "Passpost",
        2: "Govement ID",
    }
    occupationtypetitle = {
        1: "Officer",
        2: "Docter, Pharmacist, nurse",
        3: "Civil Servent",
        4: "Engineer, Construction worker, Mechanical Engineer",
        5: "Armed Forces",
        6: "Freelancer",
        7: "Pupil, Student",
        8: "Other",
    }
    jobtitle = {
        1: "Staff",
        2: "Manager, Supervisor",
        3: "Director, Senior Manager",
        4: "Business Owner",
    }
    fake = faker.Faker()

    def __init__(
        self,
        customer_type,
        customerrecordid,
        customerid,
        passpost_identitynumber,
        govementid_identitynumber,
        taxcode,
    ):
        if customer_type < min(
            list(self.customer_type.keys())
        ) or customer_type > min(list(self.customer_type.keys())):
            __customerType = random.choice(list(self.customer_type.keys()))
        else:
            __customerType = customer_type
        self.customerrecordid = customerrecordid
        self.customerid = customerid
        self.customertype = self.customer_type[__customerType]
        __gender = random.choice(list(self.gender.keys()))
        if __gender == 1:
            self.name = self.fake.name_male()
        elif __gender == 2:
            self.name = self.fake.name_female()

        self.birthday = self.fake.date()
        self.gender = self.gender[__gender]
        self.nationality = self.fake.country()
        self.residentstatus = random.choice([0, 1])
        if self.residentstatus == 0:
            self.residentperiodinvn = (
                self.fake.date_time_this_decade().strftime("%Y-%m-%d")
            )
        else:
            self.residentperiodinvn = ""
        __personal_identitytype = random.choice(
            list(self.personal_identityType.keys())
        )
        self.identitytype = self.personal_identityType[__personal_identitytype]
        self.issueby = ""
        if __personal_identitytype == 1:
            self.identitynumber = passpost_identitynumber
        elif __personal_identitytype == 2:
            self.identitynumber = govementid_identitynumber

        self.issuedate = self.fake.date_time_between(
            start_date=datetime.strptime(self.birthday, "%Y-%m-%d")
        ).strftime("%Y-%m-%d")
        self.expdate = self.fake.date_time_between(
            start_date=datetime.strptime(self.issuedate, "%Y-%m-%d")
        ).strftime("%Y-%m-%d")
        self.taxcode = taxcode
        self.occupationtypetitle = random.choice(
            list(self.occupationtypetitle.values())
        )
        if self.occupationtypetitle == "Other":
            self.otheroccupationdescriptions = ""
        else:
            self.otheroccupationdescriptions = ""
        self.jobtitle = random.choice(list(self.jobtitle.values()))
        if self.jobtitle == "Other":
            self.otherjobdescriptions = self.fake.job()
        else:
            self.otherjobdescriptions = ""
        self.permanentaddvn = self.fake.address()
        self.overseaadd = self.fake.address()
        self.currentaddvn = self.fake.address()
        self.timeatcurrentaddress = random.randrange(20)
        self.email = self.fake.email()
        self.phonenumber = self.fake.phone_number()
        if __customerType == 2:
            self.linkedcompany = random.randrange(customerid)
            self.linkeduser = ""
        elif __customerType == 3:
            self.linkedcompany = ""
            self.linkeduser = random.randrange(customerid)
        else:
            self.linkedcompany = ""
            self.linkeduser = ""
        self.updatedate = self.fake.date_time_between(
            start_date=datetime.strptime(self.birthday, "%Y-%m-%d")
        ).strftime("%Y-%m-%d, %H:%M:%S")

    def toList(self):
        return [
            Row(
                self.customerrecordid,
                self.customerid,
                self.customertype,
                self.name,
                self.birthday,
                self.gender,
                self.nationality,
                self.residentstatus,
                self.residentperiodinvn,
                self.identitytype,
                self.issueby,
                self.identitynumber,
                self.issuedate,
                self.expdate,
                self.taxcode,
                self.occupationtypetitle,
                self.otheroccupationdescriptions,
                self.jobtitle,
                self.otherjobdescriptions,
                self.permanentaddvn,
                self.overseaadd,
                self.currentaddvn,
                self.timeatcurrentaddress,
                self.email,
                self.phonenumber,
                self.linkedcompany,
                self.updatedate,
            )
        ]


class FakeCustomerGenerator:
    def __init__(self):
        self.spark_stoped = False
        self.fact_customer = []
        self.spark = SparkSession.builder.appName(
            "Python Spark Customer Fact Generator"
        ).getOrCreate()

        self.id_config = {
            "customerrecordid": 10000000,
            "customerid": 10000000,
            "passpost_identitynumber": 111111111,
            "govementid_identitynumber": 111111111,
            "company_identitynumber": 111111111,
            "taxcode": 0,
        }
        for file in self.id_config.keys():
            file_exist = os.path.exists(CONFIG_FILE_PATH_PREFIX + file)
            if file_exist:
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "r")
                self.id_config[file] = int(realFile.readlines()[0])
                realFile.close()
            else:
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "w+")
                realFile.seek(0)
                realFile.write(str(self.id_config[file]))
                realFile.close()
            file_exist = os.path.exists(
                CONFIG_FILE_PATH_PREFIX + "default" + file
            )
            if not file_exist:
                realFile = open(
                    CONFIG_FILE_PATH_PREFIX + "default" + file, "w+"
                )
                realFile.seek(0)
                realFile.write(str(self.id_config[file]))
                realFile.close()

    def __del__(self):
        self.stop_spark()

    def start_spark(self):
        if self.spark_stoped:
            self.spark = SparkSession.builder.appName(
                "Python Spark Customer Fact Generator"
            ).getOrCreate()
            self.spark_stoped = False

    def stop_spark(self):
        if not self.spark_stoped:
            self.spark.stop()
            self.spark_stoped = True

    def create_data(self, NumbefOfRow):
        for _ in range(NumbefOfRow):
            bank_customer_record = BankCustomer(
                1,
                self.id_config["customerrecordid"],
                self.id_config["customerid"],
                self.id_config["passpost_identitynumber"],
                self.id_config["govementid_identitynumber"],
                self.id_config["taxcode"],
            )
            self.id_config["customerrecordid"] += 1
            self.id_config["customerid"] += 1
            if (
                bank_customer_record.identitytype
                == BankCustomer.personal_identityType[1]
            ):
                self.id_config["passpost_identitynumber"] += 1
            elif (
                bank_customer_record.identitytype
                == BankCustomer.personal_identityType[2]
            ):
                self.id_config["govementid_identitynumber"] += 1
            self.id_config["taxcode"] += 1
            self.fact_customer = (
                self.fact_customer + bank_customer_record.toList()
            )
        customer_colum_layout = StructType(
            [
                StructField("customerrecordid", StringType(), True),
                StructField("customerid", StringType(), True),
                StructField("customertype", StringType(), True),
                StructField("name", StringType(), True),
                StructField("birthday", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("nationality", StringType(), True),
                StructField("residentstatus", StringType(), True),
                StructField("residentperiodinvn", StringType(), True),
                StructField("identitytype", StringType(), True),
                StructField("issueby", StringType(), True),
                StructField("identitynumber", StringType(), True),
                StructField("issuedate", StringType(), True),
                StructField("expdate", StringType(), True),
                StructField("taxcode", StringType(), True),
                StructField("occupationtypetitle", StringType(), True),
                StructField("otheroccupationdescriptions", StringType(), True),
                StructField("jobtitle", StringType(), True),
                StructField("otherjobdescriptions", StringType(), True),
                StructField("permanentaddvn", StringType(), True),
                StructField("overseaadd", StringType(), True),
                StructField("currentaddvn", StringType(), True),
                StructField("timeatcurrentaddress", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phonenumber", StringType(), True),
                StructField("linkedcompany", StringType(), True),
                StructField("updatedate", StringType(), True),
            ]
        )
        now = datetime.now()
        self.file_name = "FactCustomer " + now.strftime("%Y-%m-%d %H-%M-%S-%f")
        self.fact_bank_acc_df = self.spark.createDataFrame(
            data=self.fact_customer, schema=customer_colum_layout
        )
        return [self.file_name, self.fact_bank_acc_df]

    def to_file(self):
        print(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        self.fact_bank_acc_df.write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        for file in self.id_config.keys():
            realFile = open(CONFIG_TMP_FILE_PATH_PREFIX + file, "w+")
            realFile.seek(0)
            realFile.write(str(self.id_config[file]))
            realFile.close()
        for file in self.id_config.keys():
            shutil.move(
                CONFIG_FILE_PATH_PREFIX + file,
                CONFIG_BACKUP_FILE_PATH_PREFIX + file,
            )
            shutil.move(
                CONFIG_TMP_FILE_PATH_PREFIX + file,
                CONFIG_FILE_PATH_PREFIX + file,
            )
        shutil.move(
            OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name,
            OUTPUT_FILE_PATH_PREFIX + self.file_name,
        )


class TransactionObject:
    tracsaction_type_dim = {
        1: "cash withdrawals",
        2: "checks",
        3: "online payments",
        4: "debitcard charges",
        5: "wire transfers",
        6: "loan payments",
    }
    comcurrency_dim = {1: "VND", 2: "USD", 3: "SIG"}
    transaction_point_dim = {1: "ATM", 2: "BankingApp", 3: "PostMachine"}
    fake = faker.Faker()

    def __init__(self, accountid):
        self.transactionid = accountid
        self.amount = random.randint(1, 10000)
        self.timestamp = self.fake.date()
        __TypeTransactionDim = random.choice(
            list(self.tracsaction_type_dim.keys())
        )
        self.typeTransaction = self.tracsaction_type_dim[__TypeTransactionDim]
        # __concurrency = random.choice(list(self.ConcurrencyDim.keys()))
        # self.concurrency = self.ConcurrencyDim[__concurrency]
        self.concurrency = "VND"
        __transactionpoint = random.choice(
            list(self.transaction_point_dim.keys())
        )
        self.transactionpoint = self.transaction_point_dim[__transactionpoint]
        __moneyMoveWay = random.choice([0, 1])
        if __moneyMoveWay == 0:
            self.destinationaccountid = self.fake.credit_card_full()
            self.sourceCardID = ""
        else:
            self.destinationaccountid = ""
            self.sourceCardID = self.fake.credit_card_full()

        self.exchangerate = 1
        self.lattitude = random.uniform(-85, 85)
        self.longtitude = random.uniform(-180, 180)

    def toList(self):
        return [
            Row(
                self.transactionid,
                self.amount,
                self.timestamp,
                self.typeTransaction,
                self.concurrency,
                self.transactionpoint,
                self.destinationaccountid,
                self.sourceCardID,
                self.exchangerate,
                self.lattitude,
                self.longtitude,
            )
        ]


class FakeTransactionGenerator:
    def __init__(self):
        self.spark_stoped = False
        self.fact_customer = []
        self.spark = SparkSession.builder.appName(
            "Python Spark Customer Fact Generator"
        ).getOrCreate()

        self.id_config = {
            "accountid": 10000000,
            "defaultaccountid": 10000000,
            "transactionid": 0,
        }
        for file in self.id_config.keys():
            file_exist = os.path.exists(CONFIG_FILE_PATH_PREFIX + file)
            if file_exist:
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "r")
                self.id_config[file] = int(realFile.readlines()[0])
                realFile.close()
            else:
                realFile = open(CONFIG_FILE_PATH_PREFIX + file, "w+")
                realFile.seek(0)
                realFile.write(str(self.id_config[file]))
                realFile.close()
            file_exist = os.path.exists(
                CONFIG_FILE_PATH_PREFIX + "default" + file
            )
            if not file_exist:
                realFile = open(
                    CONFIG_FILE_PATH_PREFIX + "default" + file, "w+"
                )
                realFile.seek(0)
                realFile.write(str(self.id_config[file]))
                realFile.close()

    def __del__(self):
        self.stop_spark()

    def start_spark(self):
        if self.spark_stoped:
            self.spark = SparkSession.builder.appName(
                "Python Spark Customer Fact Generator"
            ).getOrCreate()
            self.spark_stoped = False

    def stop_spark(self):
        if not self.spark_stoped:
            self.spark.stop()
            self.spark_stoped = True

    def create_data(self, NumbefOfRow):
        for _ in range(NumbefOfRow):
            bank_account_record = TransactionObject(
                self.id_config["accountid"]
            )
            self.id_config["accountid"] += 1
            self.fact_customer = (
                self.fact_customer + bank_account_record.toList()
            )
        transaction_column_layout = StructType(
            [
                StructField("transactionid", StringType(), True),
                StructField("amount", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("typeTransaction", StringType(), True),
                StructField("concurrency", StringType(), True),
                StructField("transactionpoint", StringType(), True),
                StructField("destinationaccountid", StringType(), True),
                StructField("sourceCardID", StringType(), True),
                StructField("exchangerate", StringType(), True),
                StructField("lattitude", StringType(), True),
                StructField("longtitude", StringType(), True),
            ]
        )
        now = datetime.now()
        self.file_name = "FactTransaction " + now.strftime(
            "%Y-%m-%d %H-%M-%S-%f"
        )
        self.fact_bank_acc_df = self.spark.createDataFrame(
            data=self.fact_customer, schema=transaction_column_layout
        )
        return [self.file_name, self.fact_bank_acc_df]

    def to_file(self):
        print(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        self.fact_bank_acc_df.write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name)
        for file in self.id_config.keys():
            realFile = open(CONFIG_TMP_FILE_PATH_PREFIX + file, "w+")
            realFile.seek(0)
            realFile.write(str(self.id_config[file]))
            realFile.close()
        for file in self.id_config.keys():
            shutil.move(
                CONFIG_FILE_PATH_PREFIX + file,
                CONFIG_BACKUP_FILE_PATH_PREFIX + file,
            )
            shutil.move(
                CONFIG_TMP_FILE_PATH_PREFIX + file,
                CONFIG_FILE_PATH_PREFIX + file,
            )
        shutil.move(
            OUTPUT_TMP_FILE_PATH_PREFIX + self.file_name,
            OUTPUT_FILE_PATH_PREFIX + self.file_name,
        )
