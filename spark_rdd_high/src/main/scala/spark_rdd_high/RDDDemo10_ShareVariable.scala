package spark_rdd_high

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object RDDDemo10_ShareVariable {

  //执行 wordcount，并过滤非单词符号并统计符号总数
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("share").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val mycounter: LongAccumulator = sc.longAccumulator("mycounter")  //计数器
    val ruleList: List[String] = List(",", "!", "@", "#", "|", "_")
    //如果没有广播变量，该 ruleList 要发送给所有的 Task，而广播变量只需要发送给每个 node
    val broadcast: Broadcast[List[String]] = sc.broadcast(ruleList)

    val lines: RDD[String] = sc.textFile("input/wordcount2.txt")
    val result: RDD[(String, Int)] =lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      filter(ch => {
        val list = broadcast.value
        if(list.contains(ch)){
          mycounter.add(1)
          false
        }else{
          true
        }
      }).
      map((_,1)).
      reduceByKey(_+_)

    result.foreach(println)
    println(mycounter.value)
  }

}
