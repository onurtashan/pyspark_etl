from pyspark import SparkContext
from pyspark import SQLContext
import time
sc = SparkContext("local", "First ETL App")


sqlContext = SQLContext(sc)

query = "(select id, name, job from OTASHAN.SPARK_INPUT) t"

#Read data from Oracle Table
read_table = sqlContext.read.format("jdbc") \
    .option("url","jdbc:oracle:thin:USERNAME/PASSWORD@//hostname:PORT/SID") \
    .option("dbtable", query) \
    .option("user","DB_USERNAME") \
    .option("password","DB_USER_PASSWORD") \
    .option("driver","oracle.jdbc.driver.OracleDriver") \
    .load()

#Group by "job" column count
job_count = read_table.groupBy("job").count()
job_count.show()

#Write result to json
start = time.time()
job_count.coalesce(1).write.json('/home/onurtashan/spark_output')
end = time.time()
print("To JSON File Time", end-start)

#Write result to Oracle Table
start2 = time.time()
job_count.write.format("jdbc") \
    .option("url","jdbc:oracle:thin:USERNAME/PASSWORD@//hostname:PORT/SID") \
    .option("dbtable", "OTASHAN.SPARK_OUTPUT") \
    .option("user","OTASHAN") \
    .option("password","OTASHAN") \
    .option("driver","oracle.jdbc.driver.OracleDriver") \
    .mode('append') \
    .save()
end2 = time.time()
print("To DB Table Time", end2-start2)

print("Done. Elapsed Time:", (end-start) + (end2-start2))