import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Solution5 extends App{
  val spark = SparkSession.builder()
    .appName("solution5")
    .master("local[*]")
    .getOrCreate()

  val df_problem = spark.read
    .format("avro")
    .load("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/problem7/employees")

  df_problem.printSchema()
  df_problem.show()

  val df_solution = df_problem.withColumn("fullName",concat_ws(" ",df_problem("fname"),df_problem("lname")))

  df_solution.show()

  df_solution.repartition(1).write
    .format("avro")
    .option("compression","snappy")
    .mode("overwrite")
    .save("/home/centos/mskorikov/solutions/results/result5")
}
