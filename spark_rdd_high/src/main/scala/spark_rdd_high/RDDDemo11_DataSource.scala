package spark_rdd_high

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo11_DataSource {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("share").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // sc.objectFile() 等读取不同给的数据格式
    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val result: RDD[(String, Int)] =lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      map((_,1)).
      reduceByKey(_+_)

    result.foreach(println)
    result.repartition(1).saveAsTextFile("output/wordcount_out")
    result.repartition(1).saveAsObjectFile("output/wordcount_out2")
    result.repartition(1).saveAsSequenceFile("output/wordcount_out3")
  }

}
