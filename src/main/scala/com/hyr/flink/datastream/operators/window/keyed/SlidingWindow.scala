package com.hyr.flink.datastream.operators.window.keyed

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/** *****************************************************************************
 *
 * @date 2021-03-30 2:11 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 滑动窗口
 ******************************************************************************/
object SlidingWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

    // 每隔5秒，统计近10秒内输入单词的次数
    val windowData: WindowedStream[(String, Int), String, TimeWindow] = stream
      .flatMap {
        _.toLowerCase.split(" ").filter {
          _.nonEmpty
        }
      }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      // 设置间隔5秒的滑动窗口
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // TODO SlidingEventTimeWindows

    val result = windowData.sum(1)

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
