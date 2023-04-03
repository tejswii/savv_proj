from pyspark.sql import SparkSession
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('/user/hive/warehouse/')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("SHOW DATABASES").show()
spark.sql("USE TSWI")

# spark is an existing SparkSession
spark.sql("CREATE TABLE IF NOT EXISTS tswi.user_dump (user_id INT, file_name STRING, time bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE TBLPROPERTIES ('skip.header.line.count'='1')")

#spark.sql("LOAD DATA INPATH '/user/hadoop/tswi/userDump/user_upload_dump_2023_03_06.csv' OVERWRITE INTO TABLE tswi.user_dump")

spark.sql("""
    INSERT INTO user_total
    SELECT 
        tab1.time_ran,
        tab1.total_users,
        tab1.total_users - COALESCE(tab2.total_users, 0) AS users_added
    FROM (
        SELECT CURRENT_TIMESTAMP() AS time_ran, COUNT(*) AS total_users
        FROM usr
    ) tab1
    LEFT JOIN (
        SELECT time_ran, total_users
        FROM user_total
    ) tab2 ON tab1.time_ran > tab2.time_ran
    ORDER BY tab1.time_ran
""")
spark.sql("select * from user_total").show();

## USER REPORT TABLE


spark.sql("""
    INSERT OVERWRITE TABLE user_report
    SELECT
        usr.id AS user_id,
        COALESCE(SUM(CASE WHEN activitylog.type = 'UPDATE' THEN 1 ELSE 0 END)) AS total_updates,
        COALESCE(SUM(CASE WHEN activitylog.type = 'INSERT' THEN 1 ELSE 0 END)) AS total_inserts,
        COALESCE(SUM(CASE WHEN activitylog.type = 'DELETE' THEN 1 ELSE 0 END)) AS total_deletes,
        MAX(activitylog.type) AS last_activity_type,
        CASE WHEN CAST(from_unixtime(MAX(activitylog.timestamp)) AS DATE)  >= DATE_SUB(CURRENT_TIMESTAMP(), 2) THEN true ELSE false END AS is_active,
        COALESCE(COUNT(user_dump.user_id)) AS upload_count
    FROM usr
    LEFT JOIN activitylog ON usr.id = activitylog.user_id
    LEFT JOIN user_dump ON usr.id = user_dump.user_id
    GROUP BY usr.id""")
spark.sql("SELECT * FROM user_report").show()
