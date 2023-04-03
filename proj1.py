from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read from MySQL").config("spark.sql.warehouse.dir","/user/hive/warehouse").enableHiveSupport().getOrCreate()


jdbcHostname = "savvients-classroom.cefqqlyrxn3k.us-west-2.rds.amazonaws.com"
jdbcPort = 3306
jdbcDatabase = "practical_exercise"
jdbcUsername = "sav_proj"
jdbcPassword = "authenticate"
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.mysql.jdbc.Driver"
}

spark.sql("USE TSWI")

df = spark.read.jdbc(url=jdbcUrl, table="user", properties=connectionProperties)
df.show()
df.write.mode('overwrite').saveAsTable("tswi.usr")
df2 = spark.read.jdbc(url=jdbcUrl, table="activitylog", properties=connectionProperties)
df2.show()
df2.write.mode('overwrite').saveAsTable("tswi.activitylog")