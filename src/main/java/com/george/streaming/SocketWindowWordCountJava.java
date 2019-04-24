package com.george.streaming;

import com.george.WordWithCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
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
public class SocketWindowWordCountJava {

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
        // 2.指定数据源
        String hostname = "192.168.20.3";
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
