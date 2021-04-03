package spark_sougouq

import com.hankcs.hanlp.HanLP
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 对搜索记录分词，并统计如下指标
 * 1.总热门搜索词
 * 2.用户热门搜索词
 * 3.各时间段搜索次数
 */
object SougouSearchLogAnalysis {

  //封装一条数据
  case class SougouRecord(queryTime: String, userId: String, queryWords: String,
                          resultRank: Int, clickRank: Int, clickUrl: String)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sougouq").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //广播
    val ruleList: List[String] = List("+", ".")
    val broadcast: Broadcast[List[String]] = sc.broadcast(ruleList)

    //读取
    val lines: RDD[String] = sc.textFile("input/SougouQ.txt")
    //封装
    val sougouRecordRDD: RDD[SougouRecord] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      SougouRecord(arr(0), arr(1), arr(2), arr(3).toInt, arr(4).toInt, arr(5))
    })
    //分词，一条 queryWords 会变成多个 word 集合，最后 flatMap 成一个 word 集合
    val wordsRDD: RDD[String] = sougouRecordRDD.flatMap(record => {
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "")
      import scala.collection.JavaConverters._
      HanLP.segment(wordsStr).asScala.map(_.word)
    })
    //统计
    val result1: Array[(String, Int)] = wordsRDD.
      filter(ch => {
        val list = broadcast.value
        if(list.contains(ch)){
          false
        }else{
          true
        }
      }).
      map((_, 1)).
      reduceByKey(_+_).
      sortBy(_._2, ascending = false).
      take(10)

    result1.foreach(println)

    println("-------------------------------")

    //分词并映射，（userId，词），（优化，先得到 userIdAndWordRDD，再从中取出 wordsRDD）
    val userIdAndWordRDD: RDD[(String, String)] = sougouRecordRDD.flatMap(record => {
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "")
      import scala.collection.JavaConverters._
      val words = HanLP.segment(wordsStr).asScala.map(_.word)

      val userId: String = record.userId
      words.map((userId, _))
    })
    //聚合排序
    val result2: Array[((String, String), Int)] = userIdAndWordRDD.
      filter(t => {
        val list = broadcast.value
        if (list.contains(t._2)) {
          false
        } else {
          true
        }
      }).
      map((_, 1)).
      reduceByKey(_ + _).
      sortBy(_._2, ascending = false).
      take(10)

    result2.foreach(println)

    println("-------------------------------")

    val result3: Array[(String, Int)] = sougouRecordRDD.
      map(_.queryTime.substring(0, 5)).
      map((_, 1)).
      reduceByKey(_+_).
      sortBy(_._2, ascending = false).
      take(10)

    result3.foreach(println)

    sc.stop()
  }
}
