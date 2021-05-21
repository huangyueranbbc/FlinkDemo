package com.hyr.flink.datastream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 *
 * @date 2021-03-13 3:44 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 读取文件
 */
object FileSource {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // val text: DataStream[String] = env.readTextFile("word")
    val text: DataStream[String] = streamEnv.readTextFile("hdfs://server1:8020/user/itemcf/input/ali_t.csv", "gbk")

    text.print()

    // 4、转换和处理数据
    import org.apache.flink.streaming.api.scala._
    val counts: DataStream[(String, Int)] = text.flatMap {
      _.toLowerCase.split(" ").filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(_._1) // 分组算子 注:也可以传数字,0或者1表示下标
      .sum(1) // 聚合累加算子

    counts.print()

    streamEnv.execute()
  }

}
