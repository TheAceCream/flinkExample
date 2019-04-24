package com.george.batch.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 外连接
 *
 * 左外连接
 * 右外连接
 * 全外连接
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/4 10:42 Time: 14:15
 */
public class BatchDemoOutJoinJava {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id,用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));

        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(4,"guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        /**
         * 针对左外连接而言
         * 注意：second这个tuple中的第二个元素可能为空
         */
        text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second==null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        System.out.println("============================================================================================================================");

        /**
         * 针对右外连接而言
         * 注意：first这个tuple中的第二个元素可能为空
         */
        text1.rightOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first==null){
                            return new Tuple3<>(second.f0,null,second.f1);
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        System.out.println("============================================================================================================================");


        text1.fullOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first==null){
                            return new Tuple3<>(second.f0,null,second.f1);
                        }else if (second==null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }

                    }
                }).print();

    }



}
