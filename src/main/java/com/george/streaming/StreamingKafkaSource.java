package com.george.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafkaSource
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/13 17:06 Time: 14:15
 */
public class StreamingKafkaSource {

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

        //设置 statebackend    单任务调整
//        env.setStateBackend(new MemoryStateBackend());
//        env.setStateBackend(new FsStateBackend("hdfs://Master:9000/flink/checkpoints"));
        //官方推荐RocksDB
//        env.setStateBackend(new RocksDBStateBackend("hdfs://Master:9000/flink/checkpoints",true));

        //1.Topic
        String topic = "t1";
        //kafka配置
        Properties prop = new Properties();
        //kafka客户端
        prop.setProperty("bootstrap.servers","Master:9092");
        //消费者组
        prop.setProperty("group.id","con1");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        //默认消费策略：读取上次保存的offset信息
        myConsumer.setStartFromGroupOffsets();
        //从最早的数据开始消费，忽略存储的offset信息
//        myConsumer.setStartFromEarliest();
        //从最新的数据开始消费，忽略存储的offset信息
//        myConsumer.setStartFromLatest();

        DataStreamSource<String> text = env.addSource(myConsumer);

        text.print().setParallelism(1);

        env.execute("StreamingKafkaSource");

    }

}
