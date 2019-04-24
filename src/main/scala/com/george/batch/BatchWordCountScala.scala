package com.george.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/3/4 11:19
  * Time: 14:15
  */
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\data\\file"
    val outPut = "D:\\data\\result2"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    //！！！注意：必须添加这行 隐式转换 防止flatmap方法执行报错
    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty).map((_,1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outPut,"\n"," ").setParallelism(1)

    env.execute("batch word count")

  }

}
