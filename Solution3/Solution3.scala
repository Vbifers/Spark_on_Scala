import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Solution3 extends App{
  val spark = SparkSession.builder()
    .appName("solution3")
    .master("local[*]")
    .getOrCreate()

  val df_problem = spark.read
    .option("header",true)
    .option("delimiter","\t")
    .csv("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/problem3/eployees")

  df_problem.show()

  val df_problem2 = df_problem.withColumn("alias", concat(substring(df_problem("fname"),1,1),df_problem("lname")))

  df_problem2.write.mode("overwrite")
    .option("compression","snappy")
    .orc("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/solution3")
}