from pyspark.sql import SparkSession,Row
import yaml
import os.path

if __name__ == '__main':
    #create sparksession
    spark = SparkSession \
        .builder \
        .appName("rdd2df") \
        .master("Local[*]") \
        .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    spark.sparkContex.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "/application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    config = open(app_config_path)
    app_config = yaml.load(config,Loader=yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret = yaml.load(secret,Loader=yaml.FullLoader)

    #setup to spark to S3
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_config.set("fs.s3a.access.key", app_secret["s3_config"]["access_key"])
    hadoop_config.set("fs.s3a.secret.key", app_secret["s3_config"]["secret_access_key"])

    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_config["s3_config"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record : record.find("txn_id")) \
        .map(lambda record : record.split("|")) \
        .map(lambda record : (int(record[0]),record[1], float(record[2]), record[3], record[4], record[5], record[6]))

    for rec in txn_fct_rdd.take(5):
        print(rec)

    print("\nConvert RDD to Dataframe using toDF() - without column names,")

    txnDfNoColNames = txn_fct_rdd.toDF()
    txnDfNoColNames.printschem()
    txnDfNoColNames.show(4,False)

    print("\nCreating Dataframe out of RDD without column names using createDataframe(),")

    txnDfNoColNames2 = spark.createDataFrame(txn_fct_rdd)
    txnDfNoColNames2.printschem()
    txnDfNoColNames2.show(5,False)

    print("\nConvert RDD to Dataframe using toDF(colNames: String*) - with column names,")
    txnDfNoColNames3 = txn_fct_rdd.toDF(["txn_id", "created_time_string", "amount", "cust_id", "status", "merchant_id", "created_time_ist"])
    txnDfNoColNames3.printSchema()
    txnDfNoColNames3.show(4,False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/rdd/rdd2df1_thru_sch_autoinfer.py











