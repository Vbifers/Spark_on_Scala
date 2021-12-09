import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

object Solution1 extends App {
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("Solution1")
    .getOrCreate();

  //create schemas
  val order = new StructType()
    .add("order_id",IntegerType,true)
    .add("order_date",StringType,true)
    .add("order_customer_id",IntegerType,true)
    .add("order_status",StringType,true)

  val orderItem = new StructType()
    .add("order_item_id",IntegerType,true)
    .add("order_item_order_id",IntegerType,true)
    .add("order_item_product_id",IntegerType,true)
    .add("order_item_quantity",IntegerType,true)
    .add("order_item_subtotal",DoubleType,true)
    .add("order_item_product_price",DoubleType,true)

  val customer = new StructType().add("customer_id",IntegerType,true)
    .add("customer_fname",StringType,true)
    .add("customer_lname",StringType,true)
    .add("customer_email",StringType,true)
    .add("customer_password",StringType,true)
    .add("customer_street",StringType,true)
    .add("customer_city",StringType,true)
    .add("customer_state",StringType,true)
    .add("customer_zipcode",StringType,true )

  //create DF
  val orderDf = spark.read
    .option("header","false")
    .option("sep",",")
    .schema(order)
    .csv("/tmp/spark_practics/data/orders.csv")

  val orderItemsDf = spark.read
    .option("header","false")
    .option("sep",",")
    .schema(orderItem)
    .csv("/tmp/spark_practics/data/order_items.csv")

  val custDf = spark.read
    .option("header","false")
    .option("sep",",")
    .schema(customer)
    .csv("/tmp/spark_practics/data/customers.csv")

  val res = orderItemsDf.groupBy("order_item_order_id").sum("order_item_subtotal")
    .join(orderDf,orderDf("order_id") === orderItemsDf("order_item_order_id"))
    .filter("order_status='COMPLETE'")
    .join(custDf,orderDf("order_customer_id")===custDf("customer_id"))
    .withColumn("customer_alias",concat_ws(" ",substring(custDf("customer_fname"),0,1),custDf("customer_lname")))
    .select("customer_alias", "sum(order_item_subtotal)").toDF("customer_alias","order_revenue")

  res.repartition(1).write.format("parquet")
    .option("compress","snappy")
    .mode("overwrite")
    .save("hdfs://ip-10-3-100-4.eu-west-1.compute.internal:8020/tmp/spark_practics/task_b1/solution")

}