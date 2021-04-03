package spark_streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 单批统计
 */
object WordCount01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))  // 5s 一批数据

    //运行之前需要在 node1 执行 nc -lk 9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKey(_+_)

    resultDS.print()

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }

}
