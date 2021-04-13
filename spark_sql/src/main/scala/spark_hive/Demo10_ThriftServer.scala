package spark_hive

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object Demo10_ThriftServer {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn: Connection = DriverManager.getConnection(
      "jdbc:hive2://node3:10000/mytest",
      "root", "java521....")

    val sql: String = "select * from dept"
    val ps: PreparedStatement = conn.prepareStatement(sql);
    val rs: ResultSet = ps.executeQuery()
    while(rs.next()){
      val deptno: String = rs.getString(1)
      val dname: String = rs.getString(2)
      val loc: Int = rs.getInt(3)
      println(s"$deptno|$dname|$loc")
    }

    if(rs != null) rs.close()
    if(ps != null) ps.close()
    if(conn != null) conn.close()
  }
}
