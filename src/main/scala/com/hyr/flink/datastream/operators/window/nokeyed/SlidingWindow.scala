package com.hyr.flink.datastream.operators.window.nokeyed

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 *
 * @date 2021-03-30 7:55 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 滑动窗口 聚合操作
 */
object SlidingWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    stream.print()

    // 每隔3秒，统计近5秒内所有通话的总通话时长
    val windowData: AllWindowedStream[StationLog, TimeWindow] = stream
      // 设置间隔5秒的滑动窗口
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3))) // TODO SlidingEventTimeWindows

    val result = windowData.aggregate(new MyCustomAggregate())

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

  /**
   * AggregateFunction增量窗口
   */
  class MyCustomAggregate extends AggregateFunction[StationLog, Long, Long] {

    /**
     * 初始化累加器，窗口开始的时候执行
     */
    override def createAccumulator(): Long = {
      0
    }

    /**
     * 累加,来一条数据处理一次
     */
    override def add(value: StationLog, accumulator: Long): Long = {
      accumulator + value.duration
    }

    /**
     * 返回累加器结果，窗口结束时执行
     */
    override def getResult(accumulator: Long): Long = {
      accumulator
    }

    /**
     * 合并
     */
    override def merge(a: Long, b: Long): Long = {
      a + b
    }
  }

}
