package com.hyr.flink.common.watermarkgenerator

import com.hyr.flink.common.StationLog
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkOutput}

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