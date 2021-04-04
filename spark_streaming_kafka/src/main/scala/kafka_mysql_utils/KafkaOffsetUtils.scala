package kafka_mysql_utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable.Map

object KafkaOffsetUtils {

  //将消费者组的 offset 保存到 mysql
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]): Unit = {
    val conn: Connection = DriverManager.getConnection( //自动加载数据库驱动
      "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
      "root", "java521....")
    val sql: String = "REPLACE INTO t_offset (topic, part, groupid, offset) VALUES (?, ?, ?, ?)"  //注意 replace
    val ps: PreparedStatement = conn.prepareStatement(sql)

    //将 RDD 强转为 offsetRange 之后，每条 record 对应 o，方法对应属性（如 record.topic() 对应 o.topic）
    for(o <- offsetRange){
      ps.setString(1, o.topic)
      ps.setInt(2, o.partition)
      ps.setString(3, groupid)
      ps.setLong(4, o.untilOffset)  //最新 offset
      ps.executeUpdate()
    }
    ps.close()
    conn.close()
  }

//    create table t_offset(
//      topic varchar(255) not null,
//      part int(11) not null,
//      groupid varchar(255) not null,
//      offset bigint(20) default null,
//      primary key(topic, part, groupid)
//    ) engine=InnoDB default charset=utf8;

  //从 mysql 读取 offset（主题分区，offset）
  def getOffsetMap(groupid: String, topic: String): Map[TopicPartition, Long] = {
    val conn: Connection = DriverManager.getConnection( //自动加载数据库驱动
      "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
      "root", "java521....")
    val sql: String = "SELECT * FROM t_offset WHERE groupid=? and topic=?"
    val ps: PreparedStatement = conn.prepareStatement(sql)
    ps.setString(1, groupid)
    ps.setString(2, topic)
    val rs: ResultSet = ps.executeQuery()
    val offsetMap: Map[TopicPartition, Long] = Map[TopicPartition, Long]()
    while(rs.next()){
      offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("part")) ->
        rs.getLong("offset")
    }
    rs.close()
    ps.close()
    offsetMap
  }
}
