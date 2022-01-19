#Read data from mysql - transactionsync,create dataframe out of it
#Add a column 'ins_dt' - current_date()
#Write a dataframe in s3 partition by ins_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import yaml
import com.utils.aws_utils as ut

if __name__ =='__main__':

    current_dir=os.path.abspath(os.path.dirname(__file__))
    app_config_path=os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path=os.path.abspath(current_dir+ "/../../" + ".secrets")

    conf=open(app_config_path)
    app_conf=yaml.load(conf, Loader=yaml.FullLoader)
    secret=open(app_secrets_path)
    app_secret=yaml.load(secret, Loader=yaml.FullLoader)

    # Create SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read files from MySQL") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('Error')

    #spark connection to S3
    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list=app_conf['source_list']
    for src in src_list:
        src_conf=app_conf[src]

        if src== 'SB':
            print("Reading SB data from MySql")
            txn_df=ut.read_from_mysql(spark,src_conf,app_secret)\
                    .withColumn("ins_dt", current_date())

            print("Writing SB data to S3")
            #mysql write
            txn_df.write\
                    .mode("overwrite")\
                    .partitionBy("ins_dt")\
                    .parquet("s3a://" +app_conf["s3_conf"]["s3_bucket"]+ "/"+app_conf["s3_conf"]["staging_dir"]+ "/" + src)

            print("Writing SB data to S3 completed")

        elif src=='OL' :
            print("Reading OL data from SFTP")

            ol_txn_df=ut.read_data_sftp(spark,src_conf,app_secret,
                                     os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]))\
                                    .withColumn("ins_dt", current_date())

            ol_txn_df.show()
            #Writing data SFTP
            print("Writing OL data to S3")

            ol_txn_df.write\
                    .mode("overwrite")\
                    .partitionBy("ins_dt")\
                    .parquet("s3a://" +app_conf["s3_conf"]["s3_bucket"]+ "/"+app_conf["s3_conf"]["staging_dir"]+ "/" + src)

            print("Writing OL data to S3 completed")

        elif src=='CP':
            print("Reading CP data from S3")

            cp_df=ut.read_from_s3(spark,src_conf) \
                .withColumn("ins_dt", current_date())
            cp_df.show()

            print("Writing data to CP data S3")

            cp_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt")\
                .parquet("s3a://" +app_conf["s3_conf"]["s3_bucket"]+ "/"+app_conf["s3_conf"]["staging_dir"]+ "/" + src)

            print("Writing CP data to S3 completed")


        elif src=='ADDR':
            print("Reading ADDR data from Mongodb")
            cust_addr_df=ut.read_from_mongo(spark,src_conf)\
                    .withColumn("ins_dt", current_date())

            cust_addr_df.show()
            cust_addr_df=cust_addr_df\
                    .select(col("consumer_id"),
                            col("mobile-no"),
                            col("address.state").alias("state"),
                            col("address.city").alias("city"),
                            col("address.street").alias("street"),
                            col("ins_dt"))

            print("Writing ADDR data S3")

            cust_addr_df.write\
                .mode("overwrite")\
                .partitionBy("ins_dt")\
                .parquet("s3a://" +app_conf["s3_conf"]["s3_bucket"]+ "/"+app_conf["s3_conf"]["staging_dir"]+ "/" + src)

            print("Writing ADDR data to S3 completed")
#spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/pg/source_data_loading.py












