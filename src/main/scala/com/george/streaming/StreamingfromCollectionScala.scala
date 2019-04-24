package com.george.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/3/20 16:40
  * Time: 14:15
  */
object StreamingfromCollectionScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = List(10,15,20)

    val text = env.fromCollection(data)

    //针对map接受数据执行加1的操作
    val num = text.map(_+1)

    num.print().setParallelism(1)

    env.execute("StreamingfromCollectionScala")

    //注意要import 隐式转换！

  }
}
