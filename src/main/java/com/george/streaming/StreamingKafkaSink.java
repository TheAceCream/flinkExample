package com.george.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * kafkaSink
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/13 17:06 Time: 14:15
 */
public class StreamingKafkaSink {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkPoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //官方推荐RocksDB
//        env.setStateBackend(new RocksDBStateBackend("hdfs://Master:9000/flink/checkpoints",true));

        DataStreamSource<String> text = env.socketTextStream("Master", 9001, "\n");

        String brokerList = "Master:9092";
        String topic = "t1";

//        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);

        //设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");

        //使用仅一次语义的kafkaProducer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(topic,new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),prop,FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        text.addSink(myProducer);

        env.execute("StreamingFromCollection");

    }

}
