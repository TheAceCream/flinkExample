package com.george.streaming.streamAPI;

import com.george.streaming.customerSource.MyNoParalleSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Union 演示
 * 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，所有合并的流类型必须是一致的。
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/13 17:06 Time: 14:15
 */
public class StreamingDemoUnion {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1); //注意针对此source，并行度只能设置为1

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        // 把text1 和 text2 组装到一起
        DataStream<Long> text = text1.union(text2);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接受到数据："+value);
                return value;
            }
        });

        //每2秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        String jobName = StreamingDemoUnion.class.getSimpleName();
        sum.print().setParallelism(1);
        env.execute(jobName);

    }

}
