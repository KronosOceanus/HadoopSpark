package spark_sql_type

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * 加载和写入数据
 * 底层是 load 和 save
 * ses.read.format("格式").load(path)
 * df.write.format("格式").save(path)
 */
object Demo01_Data {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    ses.sparkContext.setLogLevel("WARN")

    val df1: DataFrame = ses.read.text("input/data.txt")
    val df2: DataFrame = ses.read.json("input/data.json")
//    df1.dropDuplicates("value") //根据列名去重

    df2.write.mode(SaveMode.Overwrite).csv("output/data")

    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "java521....")
    df2.write.mode(SaveMode.Overwrite). //覆盖
      jdbc("jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
        "t_person", prop)  //表如果不存在则自动创建

    df1.printSchema() //打印列名
    df1.show()
    df2.printSchema()
    df2.show()

    ses.stop()
  }
}
