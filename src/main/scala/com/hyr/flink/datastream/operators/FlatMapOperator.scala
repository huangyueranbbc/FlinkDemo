package com.hyr.flink.datastream.operators

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @date 2021-03-18 4:01 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: FlatMap
 */
object FlatMapOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    val result: DataStream[Any] = dataStream.flatMap(t => ArrayBuffer(t.sid, t.callIn, t.callOut, t.callTime, t.callType, t.duration))

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
