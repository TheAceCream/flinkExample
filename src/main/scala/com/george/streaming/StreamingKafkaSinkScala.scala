package com.george.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/5/6 18:25
  * Time: 14:15
  */
object StreamingKafkaSinkScala {
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

    val text = env.socketTextStream("Master",9001,'\n')

    val topic = "t1"
    val prop = new Properties()
    //kafka客户端
    prop.setProperty("bootstrap.servers", "Master:9092")
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    //使用支持仅一次语义的形式
    val myProducer = new FlinkKafkaProducer011[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
    text.addSink(myProducer)

    env.execute("StreamingKafkaSourceScala")


  }
}
