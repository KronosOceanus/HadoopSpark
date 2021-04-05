package scala_trait

object _05TraitDemo {

  trait Handle{
    def handle(data: String): Unit = println("处理支付数据……")
  }

  trait Validate extends Handle{
    override def handle(data: String): Unit = {
      println("签名校验……")
      super.handle(data)
    }
  }

  trait Validate2 extends Handle{
    override def handle(data: String): Unit = {
      println("数据校验……")
      super.handle(data)
    }
  }

  // trait 从左到右构造，从右到左调用
  class PayService extends Validate2 with Validate{  //责任链从后往前调用（先签名校验）
    override def handle(data: String): Unit = {
      println("准备支付……")
      super.handle(data)
    }
  }

  def main(args: Array[String]): Unit = {
    val payService = new PayService
    payService.handle("支付数据")
  }
}
