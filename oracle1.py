# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host= "jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"

uname="myuser"
pwd="mypassword"

df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","emp").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()

res=df.na.fill(0,['comm']).withColumn("comm",col('comm').cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy-MM-dd"))
res.show()
res.printSchema()