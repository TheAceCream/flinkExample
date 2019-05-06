package com.george.streaming;

import com.george.WordWithCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算：
 *
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现 每隔一秒对最近两秒内数据进行汇总计算
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/2/27 16:29 Time: 14:15
 */
public class SocketWindowWordCountJavaCheckPoint {

    public static void main(String[] args) throws Exception {
        //获取需要的端口号
        int  port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("no port set. use default port 9001 ---java");
            port = 9001;
        }
        // 1.获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //check point
        // 每隔1000ms 启动一个检查点【设置check point周期】
        env.enableCheckpointing(5000);
        // 高级选项：
        // 设置模式为exactly once(默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500ms 的间隔 【check point最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【check point的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 一旦Flink处理程序被cancel后，会保留checkpoint的数据，以便根据实际需要回复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置 statebackend    单任务调整
//        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://Master:9000/flink/checkpoints"));
//        env.setStateBackend(new RocksDBStateBackend("hdfs://Master:9000/flink/checkpoints",true));

        // 2.指定数据源
        String hostname = "Master";
        String delimiter = "\n";
        // 3.连接socket 获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
        // 4.计算数据： 指定操作数据的transaction算子
        SingleOutputStreamOperator<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String words : splits) {
                    out.collect(new WordWithCount(words,1L));
                }
            }
        }).keyBy("word")
                //指定时间大小为2秒，指定时间间隔为1秒
                .timeWindow(Time.seconds(2),Time.seconds(1))
                //使用 sum 或者 reduce 都可以
                .sum("count");
//                .reduce(new ReduceFunction<WordWithCount>() {
//                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
//                        return new WordWithCount(a.getWord(),a.getCount()+b.getCount());
//                    }
//                });

        // 5.把数据打印到控制台 并设置并行度
        windowCounts.print().setParallelism(1);
        // 6.执行 （一定要实现，否则程序不执行）
        env.execute("Socket window count");
    }






}
