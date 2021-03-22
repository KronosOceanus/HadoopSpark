package spark_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_bak {

  def main(args: Array[String]): Unit = {
    //本地模式（以前写的 mapreduce 也是本地模式）
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("wordcount.txt")
    val words: RDD[String] = lines.flatMap(_.split(","))
    val wordOnes: RDD[(String, Int)] = words.map((_, 1))
    val result: RDD[(String, Int)] = wordOnes.reduceByKey(_+_)

    result.foreach(println)
    println(result.collect().toBuffer)  //本地集合再输出
    result.repartition(1).saveAsTextFile("ouput/wordcount_out")
  }
}
