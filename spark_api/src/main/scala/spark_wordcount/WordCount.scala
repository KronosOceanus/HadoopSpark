package spark_wordcount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI

object WordCount {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      println("传入输入输出路径！")
      System.exit(1)
    }

    //打包到 yarn
    val conf: SparkConf = new SparkConf().setAppName("wc")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split(","))
    val wordOnes: RDD[(String, Int)] = words.map((_, 1))
    val result: RDD[(String, Int)] = wordOnes.reduceByKey(_+_)

    result.foreach(println)
    println(result.collect().toBuffer)  //本地集合再输出
//    System.setProperty("HADOOP_USER_NAME", "root")
    val path: Path = new Path(args(1))
    val fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration)
    if (fileSystem.exists(path)) fileSystem.delete(path, true)
    result.repartition(1).saveAsTextFile(args(1))
  }
}
