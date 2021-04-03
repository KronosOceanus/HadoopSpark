package spark_rdd_high

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object RDDDemo12_JDBC {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("share").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    create table rdd_jdbc(
//      id int auto_increment,
//      name varchar(20),
//      age int,
//      primary key(id)
//    );

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("jack",18), ("tom",19), ("rose",20)))
    //写入 mysql
    dataRDD.foreachPartition(iter => {
      //每个分区开启一次连接
      val conn: Connection = DriverManager.getConnection( //自动加载数据库驱动
        "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
        "root", "java521....")
      val sql: String = "INSERT INTO rdd_jdbc (id, name, age) VALUES (NULL, ?, ?)"
      val ps: PreparedStatement = conn.prepareStatement(sql)

      iter.foreach(t => {
        val name: String = t._1
        val age: Int = t._2
        ps.setString(1, name)
        ps.setInt(2, age)
        ps.executeUpdate()
      })
      if(conn != null) conn.close()
    })

    //从 mysql 读取
    val getConnection = () => DriverManager.getConnection( //函数类型
      "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
      "root", "java521....")
    val sql: String = "SELECT * FROM rdd_jdbc WHERE id >= ? AND id <= ?"
    //定义函数（参数类型 => 返回值类型）
    val mapRow: ResultSet => (Int, String, Int) = (r: ResultSet) => {
      val id: Int = r.getInt("id")
      val name: String = r.getString("name")
      val age: Int = r.getInt("age")
      (id, name, age)
    }
    val mytestRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD[(Int, String, Int)](
      sc,
      getConnection,
      sql,
      1,
      5,
      1,
      mapRow
    )
    mytestRDD.foreachPartition(iter => {
      iter.foreach(println)
    })
  }
}
