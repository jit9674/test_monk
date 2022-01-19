import com.utils.aws_utils as ut

def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)

def read_from_redshift(spark, jdbc_url, s3_temp_path, query):
    df=spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("query", query) \
        .option("forward_spark_s3_credentials", "true") \
        .option("tempdir", s3_temp_path) \
        .load()
    return df

def write_to_redshift(regis_dim, app_secret, s3_temp_dir,table_name):
    dm=regis_dim.coalesce(1).write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", ut.get_redshift_jdbc_url(app_secret)) \
        .option("tempdir",  s3_temp_dir) \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
    return dm


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def read_from_mysql(spark,app_conf,app_secret):
    print("Starting to read data from MySQL")
    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": app_conf["mysql_conf"]["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }
    #mysql read
    df=spark\
        .read.format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .options(**jdbc_params)\
        .load()
    return df

def read_data_sftp(spark,app_conf,app_secret,pem_file_path):
    print("Starting to read data from SFTP:")
    df=spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", app_secret["sftp_conf"]["hostname"]) \
        .option("port", app_secret["sftp_conf"]["port"]) \
        .option("username", app_secret["sftp_conf"]["username"]) \
        .option("pem", pem_file_path) \
        .option("filetype", "csv") \
        .option("delimiter", "|") \
        .load(app_conf["sftp_conf"]["directory"] + "/" + app_conf["filename"])

    return df

def read_from_s3(spark, app_conf):
    print("Starting to read data from S3:")
    df=spark.read\
        .option("header","true")\
        .option("delimiter","|")\
        .format("csv")\
        .load("s3a://"+app_conf["s3_conf"]["s3_bucket"]+ "/" + app_conf["filename"])

    return df

def read_from_mongo(spark,app_conf):
    print("Starting to read data from Mongodb:")
    df=spark.read\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .option("database", app_conf["mongodb_config"]["database"])\
            .option("collection", app_conf["mongodb_config"]["collection"])\
            .load()
    return df

