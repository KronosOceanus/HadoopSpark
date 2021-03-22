package spark_rdd

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * groupByKey.mapValues(_.sum) 先分组再聚合
 * reduceByKey 有预聚合，相当于 mapreduce 的 Combiner，减少网络传输
 */
object RDDDemo06_Agg_Key {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val wordOnes: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      map((_,1))
    //wordOnes.groupBy(_._1)
    val grouped: RDD[(String, Iterable[Int])] = wordOnes.groupByKey()  //分组
    val result: RDD[(String, Int)] = grouped.mapValues(_.sum) //聚合
    result.foreach(println)

    val result1: RDD[(String, Int)] = wordOnes.reduceByKey(_+_)
    result1.foreach(println)
    val result2: RDD[(String, Int)] = wordOnes.foldByKey(1)(_+_)
    result2.foreach(println)
    val result3: RDD[(String, Int)] = wordOnes.aggregateByKey(1)(_+_, _+_)
    result3.foreach(println)
  }
}
