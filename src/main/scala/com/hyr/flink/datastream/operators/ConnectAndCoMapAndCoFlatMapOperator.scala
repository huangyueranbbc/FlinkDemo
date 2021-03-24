package com.hyr.flink.datastream.operators

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-23 8:23 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: 合并多种不同数据类型的数据集 "Connects" two data streams retaining their types. Connect allowing for shared state between the two streams.
 *               DataStream<Integer> someStream = //...
 *               DataStream<String> otherStream = //...
 *               ConnectedStreams<Integer, String> connectedStreams = someStream.connect(otherStream);
 *               CoMap CoFlat:Similar to map and flatMap on a connected data stream
 *
 ******************************************************************************/
object ConnectAndCoMapAndCoFlatMapOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream1: DataStream[(Int, Int)] = streamEnv.fromElements((1, 1), (1, 2), (2, 2))
    val dataStream2: DataStream[Int] = streamEnv.fromElements(1, 2, 3, 4, 5)
    val result: ConnectedStreams[(Int, Int), Int] = dataStream1.connect(dataStream2)
    // 使用CoMap或者CoFlatMap
    result.map(
      t => {
        (t._1, t._2)
      },
      t => {
        (t, 0)
      }).print()

    streamEnv.execute(this.getClass.getName)
  }

}
