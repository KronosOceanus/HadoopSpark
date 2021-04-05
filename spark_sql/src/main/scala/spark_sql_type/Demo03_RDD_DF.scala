package spark_sql_type

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 自定义 Schema
 */
object Demo03_RDD_DF {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/person.txt")
    val personRDD: RDD[Row] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    //自定义 Schema
    val schema =
      StructType( //其实就是个 List
        StructField("id", IntegerType, nullable = false) ::  // false 表示不为空
          StructField("name", StringType, nullable = false) ::
            StructField("age", IntegerType, nullable = false) ::
          Nil)
    val personDF: DataFrame = ses.createDataFrame(personRDD, schema)

    personDF.printSchema()
    personDF.show()

    ses.stop()
  }
}
