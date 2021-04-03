package com.hyr.flink.datastream.operators

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-22 4:57 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: 聚合算子
 ******************************************************************************/
object AggregationOperators {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(Int, Int)] = streamEnv.fromElements((1, 1), (1, 2), (2, 2), (2, 3), (3, 4), (3, 7))

    // 根据key进行汇总求和
    val keyByData: KeyedStream[(Int, Int), Int] = dataStream.keyBy(t => t._1)
    keyByData.sum(1).print()

    streamEnv.execute(this.getClass.getName)
  }

}
