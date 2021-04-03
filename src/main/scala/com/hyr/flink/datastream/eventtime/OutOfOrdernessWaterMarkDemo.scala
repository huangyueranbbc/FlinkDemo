package com.hyr.flink.datastream.eventtime

import java.text.SimpleDateFormat

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** *****************************************************************************
 * 版权信息：安信证券股份有限公司
 * Copyright: Copyright (c) 2019安信证券股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2021-04-01 3:30 下午
 * @author: <a href=mailto:huangyr@essence.com.cn>黄跃然</a>
 * @Description: 无序的数据流处理
 * *****************************************************************************/
object OutOfOrdernessWaterMarkDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 周期性的引入WaterMark 间隔100毫秒
    streamEnv.getConfig.setAutoWatermarkInterval(100)

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    // 周期性/间断性
    val data = stream.assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
        new BoundedOutOfOrdernessGenerator(60 * 1000L)
      }
    })

    // 有序的情况,为数据流中的元素分配时间戳，并定期创建水印以表示事件时间进度。 设置事件时间
    val result = stream.assignAscendingTimestamps(_.callTime)
      // 通话成功的记录
      .filter(_.callType.equals("success"))
      .keyBy(_.sid)
      // 每隔10秒统计最近20秒内，每个基站通话时间最长的一次通话记录的基站的id、通话时长、呼叫时间 (毫秒)，已经当前发生的时间范围(20秒)  窗口范围左闭右开 延迟的数据会丢掉
      .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)))
      .reduce(new MyReduceWindowFunction(), new ReturnMaxCallTimeStationLogWindowFunction)

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

  /**
   * 增量聚合,找到通话时间最长的记录
   */
  class MyReduceWindowFunction extends ReduceFunction[StationLog] {
    override def reduce(s1: StationLog, s2: StationLog): StationLog = {
      if (s1.duration > s2.duration)
        s1
      else
        s2
    }
  }

  /**
   * 返回窗口内最大通话时间的记录
   */
  class ReturnMaxCallTimeStationLogWindowFunction extends WindowFunction[StationLog, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[StationLog], out: Collector[String]): Unit = {
      val sb = new StringBuilder
      val stationLog = input.iterator.next()
      sb.append("当前时间:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())).append("  窗口范围是:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getStart)).append("---").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd))
        .append("\n")
        .append("value:").append(stationLog)
      out.collect(sb.toString())
    }
  }

}

/**
 * 自定义周期性 Watermark 生成器
 * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 */
class BoundedOutOfOrdernessGenerator(_maxOutOfOrderness: Long) extends WatermarkGenerator[StationLog] {

  // 最大延迟间隔 n
  private val maxOutOfOrderness: Long = _maxOutOfOrderness // 3.5 秒

  // 最大事件时间 maxTime
  var currentMaxTimestamp: Long = _


  /**
   * 针对每个事件进行调用，使水印生成器可以检查并记住事件时间戳，或者根据事件本身发出水印。
   *
   * @param event          事件数据本身
   * @param eventTimestamp 时间时间错
   * @param output         输出水印
   */
  override def onEvent(event: StationLog, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    // 记录最大事件时间 可能有早到的数据?
    currentMaxTimestamp = currentMaxTimestamp.max(eventTimestamp)
    output.emitWatermark(new Watermark(event.callTime))
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
    output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }
}

/**
 * 该生成器生成的 watermark 滞后于处理时间固定量。它假定元素会在有限延迟后到达 Flink。 使用系统时间作为最大事件时间
 */
class TimeLagWatermarkGenerator extends WatermarkGenerator[StationLog] {

  val maxTimeLag = 5000L // 5 秒

  /**
   * 每个事件都会调用该方法,数据延迟，使用当前系统时间戳作为最大事件时间
   */
  override def onEvent(event: StationLog, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    // 处理时间场景下不需要实现
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
    output.emitWatermark(new Watermark(System.currentTimeMillis - maxTimeLag))
  }
}
