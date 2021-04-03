package spark_rdd

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo08_Sort {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("topn").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank).  //读取一行，过滤 null
      flatMap(_.split(",")).  //将 List(x),List(y) 变成 List(x,y)
      map((_,1)). //映射成元组 (x,1)
      reduceByKey(_+_)  //聚合

    val top1 = result.sortBy(_._2, ascending = false)
    val top2 = result.map(_.swap).sortByKey(ascending = false)
    val top3 = result.top(3)(Ordering.by(_._2))

    top1.foreach(println)
    println("-----------------")
    top2.foreach(println)
    println("-----------------")
    top3.foreach(println)
  }

}
