package com.george.batch.batchAPI

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer
/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/4/18 17:24
  * Time: 14:15
  */
object BatchDemoDistinctScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")

    val text = env.fromCollection(data)

    val flatMapData = text.flatMap(line=>{
      val words = line.split("\\W+")
      for (word<-words){
        println("单词：" + word)
      }
      words
    })

    flatMapData.distinct().print()


  }


}
