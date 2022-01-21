from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import yaml
import com.utils.aws_utils as ut
import uuid
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType,StringType

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

    def fn_uuid():
        uid= uuid.uuid1()
        return str(uid)

    FN_UUID_UDF=spark.udf\
            .register("FN_UUID" , fn_uuid,StringType())

    tgt_list= app_conf['target_list']
    print(tgt_list)

    for tgt in tgt_list:
        tgt_conf= app_conf[tgt]

        if tgt=='REGIS_DIM':
            src_list= tgt_conf['source_data']
            for src in src_list:
                file_path="s3a://" +app_conf["s3_conf"]["s3_bucket"]+ "/"+ app_conf["s3_conf"]["staging_dir"]+ "/" + src
                src_df= spark.sql("select * from parquet.`{}`".format(file_path))
                src_df.printSchema()
                src_df.show(5,  False)
                src_df.createOrReplaceTempView(src)

            print("REGIS_DIM")

            REGIS_DIM= spark.sql(app_conf["REGIS_DIM"]["loadingQuery"])
            REGIS_DIM.show(5, False)

            ut.write_to_redshift(REGIS_DIM.coalesce(1), app_secret,
                                 "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", tgt_conf['tableName'])

        elif tgt=='CHILD_DIM':
            src_list = tgt_conf['source_data']
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
                src_df = spark.sql("select * from parquet.`{}`".format(file_path))
                src_df.printSchema()
                src_df.show(5, False)
                src_df.createOrReplaceTempView(src)

                print("CHILD_DIM")

                CHILD_DIM = spark.sql(app_conf["CHILD_DIM"]["loadingQuery"])
                CHILD_DIM.show(5, False)

                ut.write_to_redshift(CHILD_DIM.coalesce(1), app_secret,
                                     "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", tgt_conf['tableName'])

        elif tgt=='RTL_TXN_FCT':
            print("Redshift data reading")
            src_list = tgt_conf['source_data']
            print(src_list)
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
                src_df=spark.sql("select * from parquet.`{}`".format(file_path))
                src_df.printSchema()
                src_df.show(5, False)
                print(src)
                src_df.createOrReplaceTempView(src)

                jdbc_url=ut.get_redshift_jdbc_url(app_secret)

                mk_df=ut.read_from_redshift(spark,jdbc_url,
                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                      "select * from {0}.{1} where ins_dt='2022-01-15'".format(app_conf['datamart_schema'],tgt_conf["target_src_table"]))

                
                mk_df.createOrReplaceTempView(tgt_conf['target_src_table'])



                RTL_TXN_FCT = spark.sql(app_conf["RTL_TXN_FCT"]["loadingQuery"])
                RTL_TXN_FCT.show(5, False)

                ut.write_to_redshift(RTL_TXN_FCT.coalesce(1), app_secret,
                                     "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", tgt_conf['tableName'])

                print("Redshift data writing completed")

























# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py






