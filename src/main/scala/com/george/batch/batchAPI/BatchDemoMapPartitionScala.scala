package com.george.batch.batchAPI

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/3/4 11:19
  * Time: 14:15
  */
object BatchDemoMapPartitionScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")

    val text = env.fromCollection(data)

    text.mapPartition(it=>{
      //创建数据库连接，建议把这块代码放到try-catch代码块中
      val res = ListBuffer[String]()
      while (it.hasNext){
        val line = it.next()
        val words = line.split("\\W+")
        for (word<-words){
          res.append(word)
        }
      }
      res
      //关闭链接
    }).print()

  }

}
