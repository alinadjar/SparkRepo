//Code for Chapter 9
// This file contains Scala code to be executed in Spark shell only.


// Example 9-1
val df = spark.read.json("products.json")
case class Product(ProductId:Int, ProductName: String, ProductCategory:String)
val ds:DataSet[Product] = df.as[Product]

val DS = spark.read.json("products.json").as[Product]
DS.show ()




// Example 9-2
import org.apache.spark._
import org.apache.spark.sql._
val config = new SparkConf().setAppName("My Spark SQL app")
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)



// Example 9-3
import org.apache.spark._
import org.apache.spark.sql._
val config = new SparkConf().setAppName("My Spark SQL app")
val sc = new SparkContext(config)
val hiveContext = new HiveContext(sc)




// Example 9-4
import org.apache.spark.sql.SparkSession
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._






// Example 9-6
import org.apache.spark.sql._
val row1 = Row("Barack Obama", "President", "United States")
val row2 = Row("David Cameron", "Prime Minister", "United Kingdom")
val presidentName = row1.getString(0)
val country = row1.getString(2)






// Example 9-7
import org.apache.spark._
import org.apache.spark.sql._
val config = new SparkConf().setAppName("My Spark SQL app")
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
case class Employee(name: String, age: Int, gender: String)
val rowsRDD = sc.textFile("file:///path/to/employees.csv")
val employeesRDD = rowsRDD.map{row => row.split(",")}
                                                .map{cols => Employee(cols(0), cols(1).trim.toInt, cols(2))}
val employeesDF = employeesRDD.toDF()





// Example 9-10
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
val config = new SparkConf().setAppName("My Spark SQL app")
val sc = new SparkContext(config)
val sqlContext = new SQLContext(sc)
val linesRDD = sc.textFile("path/to/employees.csv")
val rowsRDD = linesRDD.map{row => row.split(",")}
                                            .map{cols => Row(cols(0), cols(1).trim.toInt, cols(2))}
val schema = StructType(List(
                                                  StructField("name", StringType, false),
                                                  StructField("age", IntegerType, false),
                                                  StructField("gender", StringType, false)
                                                 )
                                              )
val employeesDF = sqlContext.createDataFrame(rowsRDD,schema)
val csvDF = spark.read.schema(schema).option("sep", ", ").option("header" ,                                                                                      "false").csv("file:///path/to/employees.csv"))
  



                                                                 
                                                                 
                                                                 
                                                                 
// Example 9-12
import org.apache.spark._
import org.apache.spark.sql._
val config = new SparkConf().setAppName("My Spark SQL app")
val sc = new SparkContext(config)
val sqlContext = new org.apache.spark.sql.hive.HiveContext (sc)
// create a DataFrame from parquet files
val parquetDF = sqlContext.read
                                               .format("org.apache.spark.sql.parquet")
                                               .load("path/to/Parquet-file-or-directory")
// create a DataFrame from JSON files
val jsonDF = sqlContext.read
                                               .format("org.apache.spark.sql.json")
                                               .load("path/to/JSON-file-or-directory")
// create a DataFrame from a table in a Postgres database
val jdbcDF = sqlContext.read
                                               .format("org.apache.spark.sql.jdbc")
                                               .options(Map(
                                                     "url" -> "jdbc:postgresql://host:port/database?user=<USER>&password=<PASS>",
                                                     "dbtable" -> "schema-name.table-name"))
                                               .load()
// create a DataFrame from a Hive table
val hiveDF = sqlContext.read.table("hive-table-name")







// Example 9-13
val jsonHdfsDF = sqlContext.read.json("hdfs://NAME_NODE/path/to/data.json")
val jsonS3DF = sqlContext.read.json("s3a://BUCKET_NAME/FOLDER_NAME/data.json")
val orcHdfsDF = sqlContext.read.orc("hdfs://NAME_NODE/path/to/data.orc")
val parquetS3DF = sqlContext.read.parquet("s3a://BUCKET_NAME/FOLDER_NAME/data.parquet")





// Example 9-14
val jdbcUrl ="jdbc:mysql://host:port/database"
val tableName = "table-name"
val connectionProperties = new java.util.Properties
connectionProperties.setProperty("user","database-user-name")
connectionProperties.setProperty("password"," database-user-password")
val jdbcDF = hiveContext.read.jdbc(jdbcUrl, tableName, connectionProperties)




// Example 9-15
val predicates = Array("country='Germany'")
val usersGermanyDF = hiveContext.read.jdbc(jdbcUrl, tableName, predicates, connectionProperties)




// Example 9-17
val teradataDBCDF = spark.read
       .format("jdbc")
      .option("url", "jdbc:teradata://localTD, TMODE=TERA")
      .option("dbtable", "dbc.tables")
      .option("user", "admin")
      .option("password", "suP3rUser")
      .load()





// Example 9-18
import org.apache.spark.sql.types._
val userSchema = StructType(List(
                                      StructField("name", StringType, false),
                                      StructField("age", IntegerType, false),
                                      StructField("gender", StringType, false)
                                      )
                                  )
val userDF = sqlContext.read
                                      .schema(userSchema)
                                      .json("path/to/user.json")
userDF.registerTempTable("user")
val cntDF = hiveContext.sql("SELECT count(1) from user")
val cntByGenderDF = hiveContext.sql(
                                           "SELECT gender, count(1) as cnt FROM user GROUP BY gender ORDER BY cnt")








// Example 9-20
case class Customer(cId: Long, name: String, age: Int, gender: String)
val customers = List(Customer(1, "James", 21, "M"),
                                 Customer(2, "Liz", 25, "F"),
                                 Customer(3, "John", 31, "M"),
                                 Customer(4, "Jennifer", 45, "F"),
                                 Customer(5, "Robert", 41, "M"),
                                 Customer(6, "Sandra", 45, "F"))
val customerDF = sc.parallelize(customers).toDF()


case class Product(pId: Long, name: String, price: Double, cost: Double)
val products = List(Product(1, "iPhone", 600, 400),
                                 Product(2, "Galaxy", 500, 400),
                                 Product(3, "iPad", 400, 300),
                                 Product(4, "Kindle", 200, 100),
                                 Product(5, "MacBook", 1200, 900),
                                 Product(6, "Dell", 500, 400))
val productDF = sc.parallelize(products).toDF()

case class Home(city: String, size: Int, lotSize: Int, bedrooms: Int, bathrooms: Int, price: Int)
val homes = List(Home("San Francisco", 1500, 4000, 3, 2, 1500000),
                                 Home("Palo Alto", 1800, 3000, 4, 2, 1800000),
                                 Home("Mountain View", 2000, 4000, 4, 2, 1500000),
                                 Home("Sunnyvale", 2400, 5000, 4, 3, 1600000),
                                 Home("San Jose", 3000, 6000, 4, 3, 1400000),
                                 Home("Fremont", 3000, 7000, 4, 3, 1500000),
                                 Home("Pleasanton", 3300, 8000, 4, 3, 1400000),
                                 Home("Berkeley", 1400, 3000, 3, 3, 1100000),
                                 Home("Oakland", 2200, 6000, 4, 3, 1100000),
                                 Home("Emeryville", 2500, 5000, 4, 3, 1200000))
val homeDF = sc.parallelize(homes).toDF()








// Cube API
case class SalesSummary(date: String, product: String, country: String, revenue: Double)
val sales = List(SalesSummary("01/01/2015", "iPhone", "USA", 40000),
                          SalesSummary("01/02/2015", "iPhone", "USA", 30000),
                          SalesSummary("01/01/2015", "iPhone", "China", 10000),
                          SalesSummary("01/02/2015", "iPhone", "China", 5000),
                          SalesSummary("01/01/2015", "S6", "USA", 20000),
                          SalesSummary("01/02/2015", "S6", "USA", 10000),
                          SalesSummary("01/01/2015", "S6", "China", 9000),
                          SalesSummary("01/02/2015", "S6", "China", 6000))
val salesDF = sc.parallelize(sales).toDF()
val salesCubeDF = salesDF.cube($"date", $"product", $"country").sum("revenue")
salesCubeDF.withColumnRenamed("sum(revenue)", "total").show(30)






// Explode API
case class Email(sender: String, recepient: String, subject: String, body: String)
val emails = List(Email("James", "Mary", "back", "just got back from vacation"),
                             Email("John", "Jessica", "money", "make million dollars"),
                             Email("Tim", "Kevin", "report", "send me sales report ASAP"))
val emailDF = sc.parallelize(emails).toDF()
val wordDF = emailDF.explode("body", "word") { body: String => body.split(" ")}
wordDF.show





// intersect API
val customers2 = List(Customer(11, "Jackson", 21, "M"),
                                      Customer(12, "Emma", 25, "F"),
                                      Customer(13, "Olivia", 31, "F"),
                                      Customer(4, "Jennifer", 45, "F"),
                                      Customer(5, "Robert", 41, "M"),
                                      Customer(6, "Sandra", 45, "F"))
val customer2DF = sc.parallelize(customers2).toDF()
val commonCustomersDF = customerDF.intersect(customer2DF)
commonCustomersDF.show







// join API
case class Transaction(tId: Long, custId: Long, prodId: Long, date: String, city: String)
val transactions = List(Transaction(1, 5, 3, "01/01/2015", "San Francisco"),
                                       Transaction(2, 6, 1, "01/02/2015", "San Jose"),
                                       Transaction(3, 1, 6, "01/01/2015", "Boston"),
                                       Transaction(4, 200, 400, "01/02/2015", "Palo Alto"),
                                       Transaction(6, 100, 100, "01/02/2015", "Mountain View"))
val transactionDF = sc.parallelize(transactions).toDF()
val innerDF = transactionDF.join(customerDF, $"custId" === $"cId", "inner")





// rollup API
case class SalesByCity(year: Int, city: String, state: String,
country: String, revenue: Double)
val salesByCity = List(SalesByCity(2014, "Boston", "MA", "USA", 2000),
                                      SalesByCity(2015, "Boston", "MA", "USA", 3000),
                                      SalesByCity(2014, "Cambridge", "MA", "USA", 2000),
                                      SalesByCity(2015, "Cambridge", "MA", "USA", 3000),
                                      SalesByCity(2014, "Palo Alto", "CA", "USA", 4000),
                                      SalesByCity(2015, "Palo Alto", "CA", "USA", 6000),
                                      SalesByCity(2014, "Pune", "MH", "India", 1000),
                                      SalesByCity(2015, "Pune", "MH", "India", 1000),
                                      SalesByCity(2015, "Mumbai", "MH", "India", 1000),
                                      SalesByCity(2014, "Mumbai", "MH", "India", 2000))
val salesByCityDF = sc.parallelize(salesByCity).toDF()
val rollup = salesByCityDF.rollup($"country", $"state", $"city").sum("revenue")
rollup.show






// Example 9-21
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







// Example 9-22
homeDF.write
      .format("parquet")
      .partitionBy("city")
      .save("homes")




// Example 9-23
val newHomesDF = sqlContext.read.format("parquet").load("homes")
newHomesDF.registerTempTable("homes")
val homesInBerkeley = sqlContext.sql("SELECT * FROM homes WHERE city = 'Berkeley'")



// Example 9-24
val jdbcUrl ="jdbc:mysql://host:port/database"
val tableName = "table-name"
val connectionProperties = new java.util.Properties
connectionProperties.setProperty("user","database-user-name")
connectionProperties.setProperty("password"," database-user-password")
customerDF.write.jdbc(jdbcUrl, tableName, connectionProperties)





// Example 9-27                        
val df = sqlContext.read.json("temperatures.json")
df.registerTempTable("citytemps")
// Register the UDF with our SQLContext
sqlContext.udf.register("CTOF", (degreesCelcius: Double) => ((degreesCelcius * 9.0 / 5.0) + 32.0))
sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM  citytemps").show()





// Example 9-29
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
 
/**
 * Computes Mean 
 */
 
//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you 
//can have CustomMean(arg1: Int, arg2: String)
class CustomMean() extends UserDefinedAggregateFunction {
 
  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("item", DoubleType)))
 
  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("sum", DoubleType),
    StructField("cnt", LongType)
  ))
 
  // Returned Data Type .
  def dataType: DataType = DoubleType
 
  // Self-explaining
  def deterministic = true
 
  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0.toDouble // set sum to zero
    buffer(1) = 0L // set number of items to 0
  }
 
  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }
 
  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
 
  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getDouble(0)/buffer.getLong(1).toDouble
  }
 
}








// Example 9-30

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
 
import com.myudafs.CustomMean
 
// define UDAF
val customMean = new CustomMean()
 
// create test dataset
val data = (1 to 1000).map{x:Int => x match {
case t if t <= 500 => Row("A", t.toDouble)
case t => Row("B", t.toDouble)
}}
 
// create schema of the test dataset
val schema = StructType(Array(
StructField("key", StringType),
StructField("value", DoubleType)
))
 
// construct data frame
val rdd = sc.parallelize(data)
val df = sqlContext.createDataFrame(rdd, schema)
 
// Calculate average value for each group
df.groupBy("key").agg(
customMean(df.col("value")).as("custom_mean"),
avg("value").as("avg")
).show()

