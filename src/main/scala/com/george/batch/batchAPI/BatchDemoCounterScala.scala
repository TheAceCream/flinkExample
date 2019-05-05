package com.george.batch.batchAPI

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/4/18 17:24
  * Time: 14:15
  */
object BatchDemoCounterScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("a","b","c","d")

    val res = data.map(new RichMapFunction[String, String] {
      // 1.定义累加器
      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        // 2.注册累加器
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        this.numLines.add(1)
        value
      }
    }).setParallelism(4)

    res.writeAsText("d:\\data\\count")
    val result = env.execute("BatchDemoCounterScala")

    val num = result.getAccumulatorResult[Int]("num-lines")
    println("num:"+num)

  }


}
