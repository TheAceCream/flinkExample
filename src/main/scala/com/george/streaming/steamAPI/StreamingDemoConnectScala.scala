package com.george.streaming.steamAPI

import com.george.streaming.customerSource.MyNoParallelSourceScala
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
object StreamingDemoConnectScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val text2_str = text2.map("str_"+_)

    val connectedStreams = text1.connect(text2_str)

    val result = connectedStreams.map(line1=>{line1},line2=>{line2})

    result.print().setParallelism(1)

    env.execute("StreamingDemoConnectScala")

    //注意要import 隐式转换！

  }
}
