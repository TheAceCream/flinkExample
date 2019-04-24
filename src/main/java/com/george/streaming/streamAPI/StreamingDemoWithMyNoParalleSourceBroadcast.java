package com.george.streaming.streamAPI;

import com.george.streaming.customerSource.MyNoParalleSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 验证分区规则 broadcast分区规则
 */
public class StreamingDemoWithMyNoParalleSourceBroadcast {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1); //注意针对此source，并行度只能设置为1

        DataStream<Long> num = text.broadcast().map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接受到数据：" + value);
                return value;
            }
        });
        //每两秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        String jobName = StreamingDemoWithMyNoParalleSourceBroadcast.class.getSimpleName();
        sum.print().setParallelism(1);
        env.execute(jobName);

    }

}
