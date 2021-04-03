package spark_hanlp

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

import java.util.List

object HanlpTest {

  def main(args: Array[String]): Unit = {
    val words: String = "[NLP入门案例]"
    val terms: List[Term] = HanLP.segment(words)
    println(terms)

    import scala.collection.JavaConverters._  // java 集合转为 scala 集合
    println(terms.asScala.map(_.word))

    val cleanWords1: String = words.replaceAll("\\[|\\]", "") //将 [] 替换为空
    println(cleanWords1)
    println(HanLP.segment(cleanWords1).asScala.map(_.word))

    val log: String = "00:00:00\t8242389147671512\t[捷克民歌土风舞++教案]\t2 3\tshwamlys.blog.sohu.com/76558184.html"
    val cleanWords2: String = log.split("\\s+")(2).
      replaceAll("\\[|\\]", "")
    println(HanLP.segment(cleanWords2).asScala.map(_.word))
  }
}
