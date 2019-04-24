package com.george.streaming.customPatition;

import com.george.streaming.customerSource.MyNoParalleSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用自定义分析
 * 根据数字的奇偶性来分区
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/20 14:17 Time: 14:15
 */
public class StreamingDemoWithMyPartition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        //对数据进行转换 Long类型转成tuple1类型
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
        //分区后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPatition(),0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("获取当前线程id"+Thread.currentThread().getId()+",value"+value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);
        env.execute("StreamingDemoWithMyParitition");


    }

}
