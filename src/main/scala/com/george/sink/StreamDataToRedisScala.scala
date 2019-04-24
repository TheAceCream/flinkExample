package com.george.sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 滑动窗口
  *
  * 每隔一秒统计最近2秒内的数据，打印到控制台
  *
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/2/27 18:24
  * Time: 14:15
  */
object StreamDataToRedisScala {

  def main(args: Array[String]): Unit = {
    //获取socket的端口号
    val port=9000

    // 1.获取运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.连接socket 获取输入数据
    val text = env.socketTextStream("192.168.20.3",port,'\n')
    // 3.解析数据(把数据打平) ，分组 ，窗口计算， 并且聚合求sum
    val l_wordsData = text.map(line=>("l_words_scala",line))

    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.20.3").setPort(6379).build()

    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper)

    l_wordsData.addSink(redisSink)

    // 5. 执行
    env.execute("Socket window count")
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{

    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }

    override def getValueFromData(data: (String, String)): String = {
      data._2
    }

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
  }

}
