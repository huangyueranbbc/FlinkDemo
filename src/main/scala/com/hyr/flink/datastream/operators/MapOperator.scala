package com.hyr.flink.datastream.operators

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 *
 * @date 2021-03-18 3:55 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: map
 */
object MapOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    dataStream.map(t => println(t)) // T ==> R

    streamEnv.execute(this.getClass.getName)
  }

}
