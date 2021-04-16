package kafka_producer

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.util.Random

/**
 * 模拟随机生成日志文件
 *
 */

object StationLogProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "node1:9092")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
    val random = new Random()
    val allStatus = Array("fail", "busy", "barring",
      "success", "success", "success", "success", "success", "success", "success", "success", "success")

    while(true){
      val callOut: String = "1860000%04d".format(random.nextInt(10000))
      val callIn: String = "1890000%04d"format(random.nextInt(10000))
      val callStatus: String = allStatus(random.nextInt(allStatus.length))
      val callDuration = if("success".equals(callStatus))(1 + random.nextInt(10)) * 1000L else 0L

      //随机生成日志数据
      val stationLog: StationLog = StationLog(
        "station_" + random.nextInt(10),
        callOut,
        callIn,
        callStatus,
        System.currentTimeMillis(),
        callDuration
      )
      println(stationLog)
      Thread.sleep(100 + random.nextInt(500))

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](
        "stationTopic", stationLog.toString)
      producer.send(record)
    }

    producer.close()
  }

  case class StationLog(
                          stationId: String,
                          callOut: String,
                          callIn: String,
                          callStatus: String,
                          callTime: Long,
                          duration: Long
                        ){
    override def toString: String = s"${stationId},${callOut},${callIn},${callStatus},${callTime},${duration}"
  }

//    /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper node1:2181
//    /usr/local/kafka/bin/kafka-topics.sh --delete --zookeeper node1:2181 --topic stationTopic
//    /usr/local/kafka/bin/kafka-topics.sh --delete --zookeeper node1:2181 --topic etlTopic
//    /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --topic stationTopic --partitions 3 --replication-factor 1
//    /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --topic etlTopic --partitions 3 --replication-factor 1
//    /usr/local/kafka/bin/kafka-console-producer.sh --topic stationTopic --broker-list node1:9092
//    /usr/local/kafka/bin/kafka-console-producer.sh --topic etlTopic --broker-list node1:9092
//    /usr/local/kafka/bin/kafka-console-consumer.sh --topic stationTopic --bootstrap-server node1:9092 --from-beginning
//    /usr/local/kafka/bin/kafka-console-consumer.sh --topic etlTopic --bootstrap-server node1:9092 --from-beginning
}
