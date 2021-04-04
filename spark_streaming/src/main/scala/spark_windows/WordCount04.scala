package spark_windows

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 窗口计算
 * 间隔 5s 计算最近 10s 的数据
 */
object WordCount04 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("windows").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))  //这里必须是窗口宽度的倍数（5 的倍数）

    //运行之前需要在 node1 执行 nc -lk 9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKeyAndWindow((a: Int, b: Int) => a+b, Seconds(10), Seconds(5))  //窗口计算

    resultDS.print()

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }

}
