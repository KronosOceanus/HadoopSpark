package spark_windows

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

/**
 * 每 10s 计算最近 20s 热搜词
 * 使用自定义输出，将结果输出到控制台 / mysql / hdfs
 */
object WordCount06 {

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
      println("topN------------------")
      top3.foreach(println)
      sortedRDD
    })

    sortedResultDS.print()  //默认输出
    sortedResultDS.foreachRDD((rdd, time) => {
      val milliseconds: Long = time.milliseconds
      println("自定义输出--------------")
      println(s"batchTime: ${milliseconds}")
      rdd.foreach(println)
      rdd.coalesce(1).  //重分区，输出到 hdfs
        saveAsTextFile(s"hdfs://node1:8020/output/time_topn_out/${milliseconds}")
      rdd.foreachPartition(iter => {  //输出到 mysql
        val conn: Connection = DriverManager.getConnection( //自动加载数据库驱动
          "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
          "root", "java521....")
        val sql: String = "INSERT INTO wordcount (time, word, count) VALUES (?, ?, ?)"
        val ps: PreparedStatement = conn.prepareStatement(sql)

        iter.foreach(t => {
          val word: String = t._1
          val count: Int = t._2
          ps.setTimestamp(1, new Timestamp(time.milliseconds))
          ps.setString(2, word)
          ps.setInt(3, count)
          ps.addBatch()
        })
        ps.executeBatch()
        if(conn != null) conn.close()
      })
    })

/**
永远记住你 永远记住你 永远记住你 永远记住你 永远记住你
上万游客挤满清明上河园石桥 上万游客挤满清明上河园石桥
外卖哥两年帮27位走失者回家
驻塞使馆凭吊北约轰炸中牺牲的烈士 驻塞使馆凭吊北约轰炸中牺牲的烈士 驻塞使馆凭吊北约轰炸中牺牲的烈士
当兵三年儿子首次回家妈妈哭成泪人 当兵三年儿子首次回家妈妈哭成泪人
 */
//    create table wordcount(
//      time timestamp not null default current_timestamp on update current_timestamp,
//      word varchar(255),
//      count int,
//      primary key(time, word)
//    );

    ssc.start() //启动
    ssc.awaitTermination()  //等待结束
    ssc.stop(stopSparkContext = true, stopGracefully = true)  //关闭时，处理完当前这一批再关
  }
}