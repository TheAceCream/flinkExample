package com.george.batch.batchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/4/29 18:12 Time: 14:15
 * 分布式缓存
 */
public class BatchDemoDisCache {


    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 注册一个文件 可以使用hdfs或者虚拟机的文件
        env.registerCachedFile("d:\\data\\count\\a.txt","a.txt");
        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            private List<String> dataList = new ArrayList();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2.使用文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println("line:"+line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //在这里可以使用dataList
                return value;
            }
        });

        result.print();

    }



}
