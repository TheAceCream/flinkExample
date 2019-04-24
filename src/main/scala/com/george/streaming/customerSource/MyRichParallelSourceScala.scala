package com.george.streaming.customerSource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * Created with IntelliJ IDEA.
  * Description:
  * 创建自定义并行度为1的source
  *
  * 实现从1开始产生递增数字
  *
  * User: weicaijia
  * Date: 2019/3/20 16:46
  * Time: 14:15
  */
class MyRichParallelSourceScala extends RichSourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long])={
      while (isRunning){
        ctx.collect(count)
        count+=1
        Thread.sleep(1000)
      }
  }

  override def cancel()={
      isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

}
