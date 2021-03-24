package com.hyr.flink.datastream.operators

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-23 8:17 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: 合并数据流,将多个数据格式一致的的数据集合并成一个数据集 dataStream.union(otherStream1, otherStream2, ...);
 * *****************************************************************************/
object UnionOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream1: DataStream[(Int, Int)] = streamEnv.fromElements((1, 1), (1, 2), (2, 2), (2, 3), (3, 4), (3, 7))
    val dataStream2: DataStream[(Int, Int)] = streamEnv.fromElements((1, 1), (2, 3), (4, 1), (5, 3), (6, 2), (7, 9))
    val result: DataStream[(Int, Int)] = dataStream1.union(dataStream2)
    result.print()

    streamEnv.execute(this.getClass.getName)
  }


}

