package spark_streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Seq

/**
 * state 恢复
 * 将之前的执行过程封装成 createFunc 方法，并返回 ssc
 * 创建 ssc 时，指定 checkpoint 和之前的执行过程函数（方法 _ 转换为函数）
 * 从 checkpoint 得到 state，从 createFunc 得到之前的执行过程
 */
object WordCount03 {

  def createFunc(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

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

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc:StreamingContext =  StreamingContext.
      getOrCreate("./checkpoint", createFunc _)  //从 checkpoint 得到 state，方法 _ 转换为函数
    ssc.sparkContext.setLogLevel("WARN")  //要重新设置日志级别

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }

}
