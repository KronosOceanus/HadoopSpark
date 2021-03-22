package spark_rdd

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo02_Basic {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      map((_,1)).reduceByKey(_+_)

    // Action
    result.foreach(println)
    result.saveAsTextFile("output/wordcount_out")
  }
}
