
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number, sum}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}

import scala.io.Source
object MyClass  extends  CommonApp {
  override def run(): Unit = {

    val customerPath = getClass.getResource("/customer.csv")
    val orderPath = getClass.getResource("/order.csv")
    val productPath = getClass.getResource("/product.csv")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("SparkSBT")
      .getOrCreate();

    val customersSchema = new StructType()
      .add("id",IntegerType,true)
      .add("customerName",StringType,true)
      .add("email",StringType,true)
      .add("joinDate",DateType,true)
      .add("status",StringType,true)


    val cust_df = spark.read
      .option("delimiter","\t")
      .schema(customersSchema)
      .csv(customerPath.getPath)

    cust_df.show(false)

    val orderSchema = new StructType()
      .add("customerId",IntegerType,true)
      .add("orderId",IntegerType,true)
      .add("productId",IntegerType,true)
      .add("numberOfProduct",IntegerType,true)
      .add("orderDate",DateType,true)
      .add("status",StringType,true)

    val ord_df = spark.read
      .option("delimiter","\t")
      .schema(orderSchema)
      .csv(orderPath.getPath)

    ord_df.show(false)

    val df_ord2 = ord_df.join(cust_df,ord_df("customerId")===cust_df("id"),"inner")

    df_ord2.show(false)

    val productSchema = new StructType()
      .add("id",IntegerType,false)
      .add("productName",StringType,true)
      .add("price",DoubleType,true)
      .add("numberOfProducts",IntegerType,true)

    val df_prod = spark.read
      .option("delimiter","\t")
      .schema(productSchema)
      .csv(productPath.getPath)

    val df_ord3 = df_ord2
      .join(df_prod,df_ord2("productId")===df_prod("id"),"left")

    df_ord3.show(false)

    val df_ord4 = df_ord3
      .groupBy("customerName","productName")
      .agg(
        sum("numberOfProduct").as("numberOfProduct")
      )

    df_ord4.show(false)

    val windowOrd = Window.partitionBy("customerName").orderBy(desc("numberOfProduct"))

    val df_ord5 = df_ord4.withColumn("row_number",row_number().over(windowOrd))
    df_ord5.show(false)

    val df_ord6 = df_ord5.filter(df_ord5("row_number")===1)

    val df_result = df_ord6.select("customerName","productName").orderBy("customerName")

    df_result.coalesce(1).write.mode(SaveMode.Overwrite).option("delimiter","\t")
      .csv("file:///home/mikhail/CSVfiles/result")

  }
}