package com.hyr.flink.datastream.operators.window.nokeyed

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/** *****************************************************************************
 *
 * @date 2021-03-30 9:34 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 滚动窗口
 *               stream
 *               .windowAll(...)           <-  required: "assigner"
 *               [.trigger(...)]            <-  optional: "trigger" (else default trigger)
 *               [.evictor(...)]            <-  optional: "evictor" (else no evictor)
 *               [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
 *               [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
 *               .reduce/aggregate/fold/apply()      <-  required: "function"
 *               [.getSideOutput(...)]      <-  optional: "output tag"
 ******************************************************************************/

object TumblingWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    // 统计5秒窗口内通话时间最长的基站信息
    val windowData: AllWindowedStream[StationLog, TimeWindow] = stream
      // 设置间隔5秒的滚动窗口
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))

    // ReduceFunction增量窗口
    val result = windowData.reduce((t1, t2) => {
      if (t1.duration > t2.duration) {
        t1
      } else {
        t2
      }
    })
    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
