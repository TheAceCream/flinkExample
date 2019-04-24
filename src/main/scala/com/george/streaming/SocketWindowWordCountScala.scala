package com.george.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 滑动窗口
  *
  * 每隔一秒统计最近2秒内的数据，打印到控制台
  *
  * Created with IntelliJ IDEA.
  * Description:
  * User: weicaijia
  * Date: 2019/2/27 18:24
  * Time: 14:15
  */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取socket的端口号
    val port:Int = try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception => {
        System.err.println("no port set. use default port 9001 ---scala")
      }
        9001
    }
    // 1.获取运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2.连接socket 获取输入数据
    val text = env.socketTextStream("192.168.20.3",port,'\n')
    // 3.解析数据(把数据打平) ，分组 ，窗口计算， 并且聚合求sum

    //！！！注意：必须添加这行 隐式转换 防止flatmap方法执行报错
    import org.apache.flink.api.scala._

    //打平，把每一行的单词都切开
    val windowCounts = text.flatMap(line => line.split("\\s"))
      //把单词转成word，1 这种形式
      .map(w => WordWithCount(w,1))
      //分组
      .keyBy("word")
      //指定窗口大小和间隔时间
      .timeWindow(Time.seconds(2),Time.seconds(1))
      //sum 或者 reduce
      .sum("count")
//      .reduce((a,b)=>WordWithCount(a.word,a.count+b.count))
    // 4.打印到控制台
    windowCounts.print().setParallelism(1)
    // 5. 执行
    env.execute("Socket window count")
  }

  case class WordWithCount(word:String,count:Long)

}
