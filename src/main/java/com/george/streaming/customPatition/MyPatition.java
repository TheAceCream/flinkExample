package com.george.streaming.customPatition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/20 14:15 Time: 14:15
 */
public class MyPatition implements Partitioner<Long> {

    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数：" + numPartitions);
        if (key % 2==0){
            return 0;
        }else {
            return 1;
        }
    }

}
