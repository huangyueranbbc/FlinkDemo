package com.hyr.flink.common.watermarkgenerator

import java.text.SimpleDateFormat

import com.hyr.flink.common.StationLog
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkOutput}

/**
 * 自定义周期性 Watermark 生成器
 * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 */
class BoundedOutOfOrdernessGenerator(_maxOutOfOrderness: Long) extends WatermarkGenerator[StationLog] {

  // 最大延迟间隔 n
  private val maxOutOfOrderness: Long = _maxOutOfOrderness

  // 最大事件时间 maxTime
  var currentMaxEventTimestamp: Long = _


  /**
   * 针对每个事件进行调用，使水印生成器可以检查并记住事件时间戳，或者根据事件本身发出水印。
   *
   * @param event          事件数据本身
   * @param eventTimestamp 时间时间错
   * @param output         输出水印
   */
  override def onEvent(event: StationLog, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    println("callTime:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(event.callTime))
    currentMaxEventTimestamp = currentMaxEventTimestamp.max(event.callTime)
    // 记录最大事件时间
    println("currentMaxTimestamp:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentMaxEventTimestamp))
    println("currentMaxTimestamp:" + currentMaxEventTimestamp)
    val watermarkTime = currentMaxEventTimestamp - maxOutOfOrderness
    println("watermarkTime:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(watermarkTime))
    println("watermarkTime:" + watermarkTime)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    // 发出的 watermark = 当前最大事件时间 - 最大延迟时间
    // Watermark = 进入 Flink 的最大的事件时间（mxtEventTime）— 指定的延迟时间（t）
    // 如果有窗口的停止时间等于或者小于 maxEventTime – t（当时的 warkmark），那么 这个窗口被触发执行。
    val watermarkTime = currentMaxEventTimestamp - maxOutOfOrderness

    output.emitWatermark(new Watermark(watermarkTime))
  }
}