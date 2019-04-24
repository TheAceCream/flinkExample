package com.george.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 把collection集合作为数据源
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/13 17:06 Time: 14:15
 */
public class StreamingFromCollection {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);

        //指定数据源
        DataStreamSource<Integer> collectionData = env.fromCollection(data);

        //通过map对数据进行处理
        DataStream<Integer> num =  collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value+1;
            }
        });
        //直接打印
        num.print().setParallelism(1);
        env.execute("StreamingFromCollection");

    }

}
