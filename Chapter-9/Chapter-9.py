# Code for Chapter 9


# Example 9-5
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    
    
    
    
# Example 9-8
from pyspark.sql import Row, SQLContext
config = SparkConf().setAppName("My Spark SQL app")
sc = SparkContext(config)
sqlContext = SQLContext(sc)
rowsRDD = sc.textFile("path/to/employees.csv")
dataFileSplitIntoColumns = rowsRDD.map(lambda l: l.split(","))
Records = dataFileSplitIntoColumns.map(lambda cols:
                            Row(name=cols[0], age=cols[1], gender=cols[2]))
#Creating a DataFrame
df= sqlContext.createDataFrame(Records)
#Perform usual DataFrame operations
df.show(5)




# Example 9-9
from pyspark.sql import Row

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")







# Example 9-11
#Reading a JSON file as a DataFrame
from pyspark.sql import SparkSession
callDetailsDF = spark.read.json("/home/spark/sampledata/json/cdrs.json")
# Write the DataFrame out as a Parquet File
callDetailsDF.write.parquet("cdrs.parquet")
# Loading the Parquet File as a DataFrame
callDetailsParquetDF = spark.read.parquet("cdrs.parquet")
# Standard DataFrame data manipulation
callDetailsParquetDF.createOrReplaceTempView("calldetails")
topCallLocsDF = spark.sql("select Origin,Dest, count(*) as cnt from
calldetails group by Origin,Dest order by cnt desc")
callDetailsParquetDF.filter(“OriginatingNum = 797303107 ”).agg({"CallCharge":"sum"}).show()

                          
                          
  
                          
# Example 9-16
# Creating a Spark session with hive Support
customSparkSession = SparkSession.builder \
            .appName("Ptyhon Sparl SQL and Hive Integration ") \
            .config("spark.sql.warehouse.dir","spark-warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
# Creating a Table
customSparkSession.sql("CREATE TABLE IF NOT EXISTS cdrs
           (callingNumber STRING, calledNumber String, origin String, Dest
            String,CallDtTm String, callCharge Int)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
# Loading Data into Hive CDRs table
customSparkSession. sql("LOAD DATA LOCAL INPATH                                                        '/home/spark/sampledata/cdrs.csv' INTO table cdrs")
# Viewing top 5 Origin destination Pairs
customSparkSession. sql(" SELECT origin, dest, count(*) as cnt from cdrs
                                          group by origin, dest order by cnt desc LIMIT 5").show()

                        
                        
                        
                        
                        
                        
# Example 9-19             
from pyspark.sql.types import *

sc = spark.sparkContext
schemaString = "name age gender"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
userSchema = StructType(fields)
userDF = spark.read.schema(userSchema).json("path/to/user.json")
userDF.createOrReplaceTempView("user")
cntByGenderDF = spark.sql("SELECT gender, count(1) as cnt FROM user GROUP BY gender ORDER BY cnt")

                        
      
                        
# Example 9-21
// save a DataFrame in JSON format
customerDF.write
      .format("org.apache.spark.sql.json")
      .save("path/to/output-directory")

// save a DataFrame in Parquet format
homeDF.write
      .format("org.apache.spark.sql.parquet")
      .partitionBy("city")
      .save("path/to/output-directory")
// save a DataFrame in ORC file format
homeDF.write
      .format("orc")
      .partitionBy("city")
      .save("path/to/output-directory")
// save a DataFrame as a Postgres database table
df.write
      .format("org.apache.spark.sql.jdbc")
      .options(Map("url" -> "jdbc:postgresql://host:port/database?user=<USER>&password=<PASS>",
                                                      "dbtable" -> "schema-name.table-name")).save()
// save a DataFrame to a Hive table
df.write.saveAsTable("hive-table-name")

df = spark.read.load("examples/src/main/resources/people.json", format="json")
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")


df = spark.read.load("examples/src/main/resources/people.csv",
                     format="csv", sep=":", inferSchema="true", header="true")

                        
                        
                        
                        
                        
 # Example 9-25                       
jdbcDF.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .save()

jdbcDF2.write \
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
          properties={"user": "username", "password": "password"})

                        
                        

                        
# Example 9-28                   
df = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")
# Register the UDF with our SQLContext
sqlContext.registerFunction("CTOF", lambda degreesCelsius: ((degreesCelsius * 9.0 / 5.0) + 32.0))
sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
