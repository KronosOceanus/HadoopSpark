package spark_streaming_kafka

import kafka_mysql_utils.KafkaOffsetUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map

/**
 * 启动时读取 mysql 是否有 offset，有则从 offset 开始消费
 * 消费一批数据之后，手动提交偏移量到 mysql
 */
object SparkStreaming_Kafka_Demo03 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092",  // kafka 集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark",  //消费者组
      "auto.offset.reset" -> "latest",  //如果有 offset 从 offset 开始消费，否则从最新记录开始消费
      "auto.commit.interval.ms" -> "1000",  //自动提交时间间隔
      "enable.auto.commit" -> (false: java.lang.Boolean)  //关闭自动提交
    )

    val topics = Array("first")  //订阅的主题，还可以用正则订阅
    //从 mysql 中读取 offset
    val offsetMap: Map[TopicPartition, Long] = KafkaOffsetUtils.getOffsetMap("spark", topics(0))
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = if(offsetMap.nonEmpty){
      println("----mysql 中存在 offset，从 offset 开始消费----")

      KafkaUtils.createDirectStream[String, String]( //直连 kafka
          ssc,
          PreferConsistent, //位置策略，有几个分区 spark 就开几个 exector 消费数据
          Subscribe[String, String](topics, kafkaParams, offsetMap) //继续消费
        )
    }else{
      println("----mysql 中不存在 offset----")

      KafkaUtils.createDirectStream[String, String]( //直连 kafka
          ssc,
          PreferConsistent, //位置策略，有几个分区 spark 就开几个 exector 消费数据
          Subscribe[String, String](topics, kafkaParams)
        )
    }

    //消费一批提交一次 offset（即一个 RDD）
    kafkaDS.foreachRDD(rdd => {
      if(! rdd.isEmpty()){
        rdd.foreach(record => {
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()
          val key: String = record.key()
          val value: String = record.value()
          println(s"topic: ${topic}, part: ${partition}, offset: ${offset}, key: ${key}, value: ${value}")
        })
        //从 rdd 获取每个分区的 offset（强转），并异步提交
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //类型转换
        //提交 offset 到 mysql
        KafkaOffsetUtils.saveOffsetRanges("spark", offsetRanges)
        println("----当前批次的 offset 已手动提交到 mysql----")
      }
    })

//    /usr/local/kafka/bin/kafka-console-producer.sh --topic first --broker-list node1:9092
//    /usr/local/kafka/bin/kafka-console-producer.sh --topic second --broker-list node1:9092

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
