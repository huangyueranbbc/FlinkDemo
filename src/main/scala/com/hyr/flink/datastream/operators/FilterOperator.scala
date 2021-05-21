package com.hyr.flink.datastream.operators

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 *
 * @date 2021-03-18 4:13 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: Filter 过滤
 */
object FilterOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    // 只筛选sid为station_5的记录
    val result: DataStream[StationLog] = dataStream.filter(t => "station_5".equals(t.sid))

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
