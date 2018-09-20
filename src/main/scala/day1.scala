object day1 {
  def main(args: Array[String]): Unit = {
    val anonymous=(x:Any)=>println(x)

    anonymous("example of anonymous function")

    val fact=factorial(7)
    println(fact)
  }
  def factorial(number:Int):Int={
    number match {
      case 0=>1
      case 1=>1
      case _=>factorial(number-1)*number
    }
  }
}
