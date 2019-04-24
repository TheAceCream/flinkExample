package com.george.batch.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/4 10:42 Time: 14:15
 */
public class BatchDemoMapPartitionJava {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);
//        text.map(new MapFunction<String, Object>() {
//            @Override
//            public Object map(String value) throws Exception {
//                //获取数据库连接 --注意，此时是没过来一条数据就获取一次连接
//                //处理数据
//                //关闭连接
//                return value;
//            }
//        });

        DataSet<String> mapPartitionData = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //获取数据库连接 --注意，此时是一个分区的数据获取一次连接【优点，每个分区获取一次连接】
                //values中保存了一个分区的数据
                //处理数据
                //关闭连接
                Iterator<String> it = values.iterator();
                while (it.hasNext()) {
                    String next = it.next();
                    String[] split = next.split("\\W+");
                    for (String word : split) {
                        out.collect(word);
                    }
                }
            }
        });

        mapPartitionData.print();

    }



}
