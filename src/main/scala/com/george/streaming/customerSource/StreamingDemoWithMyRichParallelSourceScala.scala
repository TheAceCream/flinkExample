package com.george.streaming.customerSource

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/3/20 16:40
  * Time: 14:15
  */
object StreamingDemoWithMyRichParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.addSource(new MyRichParallelSourceScala).setParallelism(2)

    val mapData = text.map(line=>{
      println("接收到的数据"+line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute("StreamingfromCollectionScala")

    //注意要import 隐式转换！

  }
}
