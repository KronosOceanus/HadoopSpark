package structured_source

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StructType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo03_File {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val csvSchema: StructType = new StructType().add("deptno", StringType, nullable = true)
      .add("dname", StringType, nullable = true)
      .add("loc", IntegerType, nullable = true)

    val df: DataFrame = ses.readStream
      .option("sep", ",")
      .option("header", "false")
      .schema(csvSchema)
      .format("csv")
      .load("input")  //动态监控该文件夹下的文件

    df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")  //不截断列
      .start()
      .awaitTermination()

    ses.close()
  }
}
