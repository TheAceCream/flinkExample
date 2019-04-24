package com.george.streaming.customerSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 *
 * 自定义实现一个支持并行度的source
 *
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/15 17:29 Time: 14:15
 */
public class MyParallelSource implements ParallelSourceFunction<Long> {
    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 主要方法
     * 启动一个Source
     * 大部分情况都要在run方法中实现一个循环，这样就可以循环产生数据了。
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消cancel的时候会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
