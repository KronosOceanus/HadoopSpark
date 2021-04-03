package spark_streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Seq

/**
 * 维护 state 实现批次统计（只能单个应用中，重启无法恢复）
 * state 存在 checkpoint 中
 * 自定义函数 updateFunc 维护 state
 */
object WordCount02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))  // 5s 一批数据

    ssc.checkpoint("./checkpoint") // state 存在 checkpoint 中

    //运行之前需要在 node1 执行 nc -lk 9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    //函数，用来维护 state，如当前批次传入 spark spark，则 currentValues = [1,1], historyValue 为历史值
    val updateFunc = (currentValues: Seq[Int], historyValue: Option[Int]) => {
      if(currentValues.nonEmpty){
        val currentResult: Int = currentValues.sum + historyValue.getOrElse(0) //如果存在则取出，不存在则得到 0
        Some(currentResult) // Some 是 Option 的子类，表示有值，否则 None（也是 Option 的子类），表示空
      }else{
        historyValue
      }
    }
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" ")).
      map((_, 1)).
      updateStateByKey(updateFunc)

    resultDS.print()

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }

}
