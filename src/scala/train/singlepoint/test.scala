package train.singlepoint

import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {
    val a=ArrayBuffer[Int](1,2,3)
//    a(0)=0
//    print(a)
//    a.toVector
    val  b=new ArrayBuffer[Int](10)
    for ( i <- 0 to 9) {
     b.append(0)
    }
    print(b)
    print(b.length)

  }

}
