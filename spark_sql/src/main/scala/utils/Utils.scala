package utils

import java.io._
import scala.util.Random

/**
 * 生成评分数据
 * 10000 个用户对 50 部电影评分
 */
object Utils {

  val DATA_SIZE: Int = 10000
  val MOVIE_SIZE: Int = 50
  val MAX_SCORE: Int = 100

  def main(args: Array[String]): Unit = {
    val bw: BufferedWriter = new BufferedWriter(new FileWriter(new File("input/movie.txt")))
    for(i <- 1 to DATA_SIZE){
      bw.write(s"${Random.nextInt(DATA_SIZE)} ${Random.nextInt(MOVIE_SIZE)} " +
        s"${Random.nextInt(MAX_SCORE)} ${System.currentTimeMillis()}")
      bw.newLine()
    }
    bw.close()
  }
}
