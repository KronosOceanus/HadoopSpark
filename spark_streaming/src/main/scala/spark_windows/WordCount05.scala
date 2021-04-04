package spark_windows

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 每 10s 计算最近 20s 热搜词
 */
object WordCount05 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("windows").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))  //这里必须是窗口宽度的倍数（10 的倍数）

    //运行之前需要在 node1 执行 nc -lk 9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKeyAndWindow((a: Int, b: Int) => a+b, Seconds(20), Seconds(10))  //窗口计算

    val sortedResultDS: DStream[(String, Int)] = resultDS.transform(rdd => {  //对底层 RDD 进行操作并返回
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, ascending = false)
      val top3: Array[(String, Int)] = sortedRDD.take(3)
      top3.foreach(println)
      sortedRDD
    })

    println("----------------------")

    sortedResultDS.print()

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }
}
/**
永远记住你 永远记住你 永远记住你 永远记住你 永远记住你
上万游客挤满清明上河园石桥 上万游客挤满清明上河园石桥
外卖哥两年帮27位走失者回家
驻塞使馆凭吊北约轰炸中牺牲的烈士 驻塞使馆凭吊北约轰炸中牺牲的烈士 驻塞使馆凭吊北约轰炸中牺牲的烈士
当兵三年儿子首次回家妈妈哭成泪人 当兵三年儿子首次回家妈妈哭成泪人
*/