package com.george.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/5/6 18:25
  * Time: 14:15
  */
object StreamingKafkaSourceScala {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //checkPoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

//    env.setStateBackend(new RocksDBStateBackend("hdfs://Master:9000/flink/checkpoints",true));


    val topic = "t1"
    val prop = new Properties()
    //kafka客户端
    prop.setProperty("bootstrap.servers", "Master:9092")
    //消费者组
    prop.setProperty("group.id", "con1")


    val myConsumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)

    val text = env.addSource(myConsumer)

    text.print()

    env.execute("StreamingKafkaSourceScala")


  }
}
