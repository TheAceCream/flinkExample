package com.george.batch.batchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * 全局累加器
 *
 * Counter计数器
 *
 * 注意只有在任务执行结束之后才能获取到累加器的值
 *
 */
public class BatchDemoCounter {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {

            // 1.创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("num-lines",numLines);
            }

//            int sum = 0;

            @Override
            public String map(String value) throws Exception {
                // 如果并行度为1 ，使用普通的累加求和即可，但是如果设置多并行度的话，则普通的累加求和结果就不准了
//                sum++;
//                System.out.println("sum:"+sum);
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(4);

//        result.print();
        result.writeAsText("d:\\data\\count");

        JobExecutionResult counter = env.execute("counter");
        int num = counter.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);


    }



}
