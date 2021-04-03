package com.hyr.flink.datastream.source

import com.hyr.flink.common.StationLog
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-13 4:02 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 集合
 * *****************************************************************************/
object CollectionSource {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      StationLog("001", "1866", "189", "busy", System.currentTimeMillis(), 0),
      StationLog("002", "1866", "188", "busy", System.currentTimeMillis(), 0),
      StationLog("004", "1876", "183", "busy", System.currentTimeMillis(), 0),
      StationLog("005", "1856", "186", "success", System.currentTimeMillis(), 20)
    ))

    stream.print()

    streamEnv.execute()
  }

}
