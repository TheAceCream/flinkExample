package com.george.streaming.streamAPI;

import com.george.streaming.customerSource.MyNoParalleSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/**
 * Split
 * 场景：
 * 可能在实际工作中，元数据流中混合了多种类型的数据，多种类型数据的处理规则不一样，所以就可以根据一定的规则，把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了。
 *
 * Created with IntelliJ IDEA. Description: User: weicaijia Date: 2019/3/13 17:06 Time: 14:15
 */
public class StreamingDemoSplit {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1); //注意针对此source，并行度只能设置为1
        //对流进行切分，按照数据奇偶性进行区分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value%2==0){
                    outPut.add("even");//偶数
                }else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });
        //select 选择一个或者多个切分后的流
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");

        DataStream<Long> moreStream = splitStream.select("even","odd");

        moreStream.rebalance();

        //打印结果
        evenStream.print().setParallelism(1);
        String jobName = StreamingDemoSplit.class.getSimpleName();
        env.execute(jobName);

    }

}
