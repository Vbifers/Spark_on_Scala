import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Solution2 extends App{
  val spark = SparkSession.builder()
  .appName("solution2")
  .master("local[*]")
  .getOrCreate()

  val df1 = spark.read.parquet("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/problem2/customers")
  df1.printSchema()
  df1.show(10)

  val df2 = df1.groupBy("country").agg(count("customer_id").as("count"))
  df2.show()

  df1.createOrReplaceTempView("customers")
  df2 = spark.sql("select country, count(1) count from customers group by country").show()

  df2.repartition(1).write
    .option("delimiter",",")
    .option("mode","overwrite")
    .option("header", true)
    .csv("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/solution2")

  spark.stop()
}
