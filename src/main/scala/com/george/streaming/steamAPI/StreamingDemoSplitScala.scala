package com.george.streaming.steamAPI

import java.{lang, util}

import com.george.streaming.customerSource.MyNoParallelSourceScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/3/20 16:40
  * Time: 14:15
  */
object StreamingDemoSplitScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.addSource(new MyNoParallelSourceScala)

    val splitStream = text.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value%2==0){
          list.add("even")
        }else{
          list.add("odd")
        }
        list
      }
    })

    val evenStream = splitStream.select("even")

    evenStream.print().setParallelism(1)

    env.execute("StreamingfromCollectionScala")

    //注意要import 隐式转换！

  }
}
