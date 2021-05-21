package com.hyr.flink.datastream.operators.window.keyed

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 *
 * @date 2021-03-30 2:15 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 会话窗口，没有重叠且没有具体的开始时间和结束时间。 当一段时间没有收到数据，会话窗口结束，当收到新的数据，会话窗口开启。
 */
object SessionWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

    // 统计会话内的单词出现次数，如果10秒没有新的输入，则结束会话窗口。当有新的单词输入，开启新的会话窗口。
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
      // 设置间隔10秒的静态会话窗口
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))

    val result = windowData.sum(1)

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
