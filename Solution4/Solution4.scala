import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Solution4 extends App{
  val spark = SparkSession.builder()
    .appName("solution4")
    .master("local[*]")
    .getOrCreate()

  val df_billings = spark.read
    .parquet("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/problem4/billings")
  val df_cust = spark.read
    .option("sep","\t")
    .option("header",true)
    .csv("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/data/problem4/customers")

  df_billings.show()
  df_cust.show()

  df_billings.createOrReplaceTempView("billings")
  df_cust.createOrReplaceTempView("customers")

  val df_solution = spark.sql(
  """select bil.customer_id, bil.transaction_id, bil.amount
    from billings bil join
    (select customer_id,min(transaction_id) as transaction_id
    from billings group by customer_id) as alone_tr
    on bil.customer_id=alone_tr.customer_id and bil.transaction_id=alone_tr.transaction_id"""
    )

  df_solution.repartition(1).write
    .format("parquet")
    .option("compression","gzip")
    .mode("overwrite")
    .save("/home/centos/mskorikov/solutions/results/result4")
}
