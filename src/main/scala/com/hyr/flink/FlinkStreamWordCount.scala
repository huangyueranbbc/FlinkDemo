package com.hyr.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * @date 2021-03-08 9:03 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Flink流计算WordCount演示 无界流
 */
object FlinkStreamWordCount {

  def main(args: Array[String]): Unit = {
    // 1、初始化流计算环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(3) // 设置默认并行度 会被算子级别的并行度覆盖

    // 2、导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 3、读取数据,读取socket流中的数据
    val stream: DataStream[String] = streamEnv.socketTextStream("server3", 8888) // DataStream ==> Dstream(spark)

    // 4、转换和处理数据
    val counts: DataStream[(String, Int)] = stream.flatMap {
      _.toLowerCase.split(" ").filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(_._1) // 分组算子 注:也可以传数字,0或者1表示下标
      .sum(1) // 聚合累加算子


    // 4、输出结果
    counts.print().setParallelism(1) // 设置算子级别并行度

    // 5、启动流计算程序
    streamEnv.execute(this.getClass.getName)
  }

}
