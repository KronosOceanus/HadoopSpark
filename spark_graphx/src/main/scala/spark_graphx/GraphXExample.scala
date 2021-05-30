package spark_graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

object GraphXExample {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 定义图结构
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )


    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)



    println("属性演示")
    println("**********************************************************")
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter {case (_, (_, age)) => age > 30}
      .collect
      .foreach {case (_, (name, age)) => println(s"$name is $age")}

    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5)
      .collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    println("列出边属性>5的tripltes：")
    for (triplet <- graph.triplets.filter(t=> t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    println("找出图中最大的出度、入度、度数即对应的顶点：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + //对某属性累积使用 max 函数
      " max of inDegrees:" + graph.inDegrees.reduce(max) +
      " max of Degrees:" + graph.degrees.reduce(max))
    println




    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点age + 10：")
    graph.mapVertices{case (id, (name, age)) => (id, (name, age+10))}.vertices
      .collect
      .foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println

    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e=>e.attr*2).edges
      .collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} attr ${e.attr}"))
    println



    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (_, vd) => vd._2 >= 30)

    println("子图所有顶点：")
    subGraph.vertices
      .collect
      .foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println

    println("子图所有边：")
    subGraph.edges
      .collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println



    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    //新建顶点类型为 User 的图
    val initialUserGraph: Graph[User, Int] = graph.mapVertices {case (_, (name, age)) => User(name, age, 0, 0)}

    //与入度，出度 RDD 连接，获取入度，出度
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (_, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (_, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
    println("连接图的属性：")
    userGraph.vertices
      .collect
      .foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    println

    println("出度和入度相同的人员：")
    userGraph.vertices.filter {case (_, u) => u.inDeg == u.outDeg}
      .collect
      .foreach {case (_, property) => println(property.name)}
    println



    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出5到各顶点的最短：")
    //单源最短路径
    val sourceId: VertexId = 5L
    //初始化距离为无穷
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(  //核心部分（3 个函数 / lambda 表达式）
      (_, dist, newDist) => math.min(dist, newDist), //节点id，节点属性，消息
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr)) //目标节点id，消息
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) //消息合并
    )
    println(sssp.vertices.collect.mkString("\n"))

    sc.stop()
  }
}