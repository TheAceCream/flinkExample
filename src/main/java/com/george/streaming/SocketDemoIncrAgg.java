package com.george.streaming;

import com.george.WordWithCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * Window增量聚合
 *
 * 数据来了算多次
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/2/27 16:29 Time: 14:15
 */
public class SocketDemoIncrAgg {

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

        DataStream<Tuple2<Integer,Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer,Integer> map(String value) throws Exception {
                return new Tuple2<>(1,Integer.parseInt(value));
            }
        });

        intData.keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        System.out.println("执行reduce操作"+value1+","+value2);
                        return new Tuple2<>(value1.f0,value1.f1 + value2.f1);
                    }
                }).print();



        // 6.执行 （一定要实现，否则程序不执行）
        env.execute("Socket window count");
    }




}
