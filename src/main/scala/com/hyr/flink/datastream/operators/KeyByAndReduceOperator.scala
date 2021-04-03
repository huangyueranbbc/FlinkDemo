package com.hyr.flink.datastream.operators

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-18 4:16 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: keyby分区和reduce汇总
 * *****************************************************************************/
object KeyByAndReduceOperator {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dataStream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    val callTimeData = dataStream.map(t => (t.sid, t.callTime))
    // 根据sid分区
    val keyByData: KeyedStream[(String, Long), String] = callTimeData.keyBy(t => t._1)
    val result = keyByData.reduce((t1, t2) => {
      println("######")
      val allcallTimes = t1._2 + t2._2 // 计算callTime总和
      println(t1._1 + ":" + t1._2 + " = " + t2._1 + ":" + t2._2 + " = " + allcallTimes)
      println("######")
      (t1._1, allcallTimes)
    })

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
