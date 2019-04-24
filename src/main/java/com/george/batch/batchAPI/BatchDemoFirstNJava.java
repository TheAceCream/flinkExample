package com.george.batch.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 *
 * 获取集合中的前N个元素
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/4 10:42 Time: 14:15
 */
public class BatchDemoFirstNJava {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id,用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"cw"));
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(1,"bk"));
        data1.add(new Tuple2<>(1,"apo"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data1);
        //获取前三条数据 按照数据插入的顺序
        text.first(3).print();
        System.out.println("========================================================================================================");

        //根据数据中的第一列进行分组，获取每组的前两个元素
        text.groupBy(0).first(2).print();
        System.out.println("========================================================================================================");

        //根据数组中的第一列分组，再根据第二列进行组内排序，获取每组的前两个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("========================================================================================================");

        //不分组，全局排序获取集合中的前三个元素
        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();

    }



}
