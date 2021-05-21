package com.hyr.flink.datastream.eventtime

import java.text.SimpleDateFormat

import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @date 2021-04-13 7:40 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: AllowedLateness  使用Flink的侧面输出功能sideOutputLateData，您可以获取最近被丢弃的数据流。
 */
object AllowedLatenessWindow {

  // 延迟数据的侧输出流标签
  lazy val DELAY_STATIONLOG: OutputTag[StationLog] = new OutputTag[StationLog]("DELAY_STATIONLOG")

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 多并行度会自动对齐WaterMark，取最小的WaterMark。避免干扰，将并行度设为1
    streamEnv.setParallelism(1)
    // 周期性的引入WaterMark 间隔100毫秒
    streamEnv.getConfig.setAutoWatermarkInterval(1000)

    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    // 如果数据延迟10秒内，WaterMark进行处理。如果数据延迟超过10秒，交给AllowedLateness处理。
    val data = stream.assignAscendingTimestamps(_.callTime).assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
        // 最长延迟10秒
        new BoundedOutOfOrdernessGenerator(10 * 1000L)
      }
    })

    // 有序的情况,为数据流中的元素分配时间戳，并定期创建水印以表示事件时间进度。 设置事件时间
    val result: DataStream[String] = data
      // 通话成功的记录
      .filter(_.callType.equals("success"))
      .keyBy(_.sid)
      // 统计最近10秒内，每个基站通话时间最长的一次通话记录的基站的id、通话时长、呼叫时间 (毫秒)，已经当前发生的时间范围(20秒)  窗口范围左闭右开 延迟的数据会丢掉
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      // 数据延迟10~20秒内，再次延迟触发窗口(注:会多次触发)。 触发条件: 窗口结束时间 > WaterMark - AllowedLateness
      .allowedLateness(Time.seconds(20))
      // 如果延迟超过20秒，则输出到侧输出流中。
      .sideOutputLateData(DELAY_STATIONLOG)
      .aggregate(new MyAggregateCountFunction, new OutPutResultWindowFunction)

    result.getSideOutput(DELAY_STATIONLOG).print("delay")
    result.print()

    streamEnv.execute(this.getClass.getName)
  }

  class MyAggregateCountFunction extends AggregateFunction[StationLog, Long, Long] {
    override def createAccumulator(): Long = {
      0
    }

    override def add(value: StationLog, accumulator: Long): Long = {
      accumulator + 1
    }

    override def getResult(accumulator: Long): Long = {
      accumulator
    }

    override def merge(a: Long, b: Long): Long = {
      a + b
    }
  }

  class OutPutResultWindowFunction extends WindowFunction[Long, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
      val value = input.iterator.next()
      val sb = new StringBuilder
      sb.append("当前时间:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())).append("  窗口范围是:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getStart)).append("---").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd))
        .append("\n")
        .append("基站ID:").append(key)
        .append("\n")
        .append("呼叫次数:").append(value)
      out.collect(sb.toString())
    }
  }

}
