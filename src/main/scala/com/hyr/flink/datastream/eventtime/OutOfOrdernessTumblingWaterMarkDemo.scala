package com.hyr.flink.datastream.eventtime

import java.text.SimpleDateFormat

import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/** *****************************************************************************
 *
 * @date 2021-04-01 3:30 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 滚动窗口的无序的数据流处理。 注：WaterMark只是决定数据窗口是否进行延迟触发。
 ******************************************************************************/
object OutOfOrdernessTumblingWaterMarkDemo {

  /**
   * 输入项：station_4,18600001941,18900003949,success,1617826650000,0
   * callTime:2021-04-08 04:17:30 1617826650000   最大延迟10秒。
   * watermarkTime:2021-04-08 04:17:20 1617826640000
   * 此时需要窗口结束时间小于watermarkTime才会触发窗口。 注：窗口结束时间是固定的，只有改变watermarkTime才能使公式生效，所以是否触发，由输入数据流中的最大事件时间决定。
   * 所以只有当新输入的数据中的最大时间>=2021-04-08 04:17:30，才会触发【2021-04-08 04:17:10 ~ 2021-04-08 04:17:30】的窗口计算。
   *
   * 输入数据窗口在【2021-04-08 04:17:00 ~ 2021-04-08 04:17:20】窗口的数据，此时最大事件时间未更新，不触发计算。
   * 窗口结束时间：1617826640000
   * 输入项：station_4,18600001941,18900003949,success,1617826630000,0
   * 输入项：station_4,18600001941,18900003949,success,1617826635000,21
   * 输入项：station_4,18600001941,18900003949,success,1617826640000,32
   * 输入项：station_4,18600001941,18900003949,success,1617826645000,34
   *
   * 输入新数据2021-04-08 04:17:31，此时更新最大事件时间，且【2021-04-08 04:17:10 ~ 2021-04-08 04:17:30】的窗口结束时间(1617826650000)<=watermarkTime(1617826660000)
   * watermarkTime=1617826660000-10000=1617826650000
   * 输入项：station_4,18600001941,18900003949,success,1617826660000,0
   * 触发【2021-04-08 04:17:10 ~ 2021-04-08 04:17:30】窗口的计算。
   * 得到station_4,18600001941,18900003949,success,1617826635000,34
   */
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 多并行度会自动对齐WaterMark，取最小的WaterMark。避免干扰，将并行度设为1
    streamEnv.setParallelism(1)
    // 周期性的引入WaterMark 间隔100毫秒
    streamEnv.getConfig.setAutoWatermarkInterval(1000)

    //读取数据源
    //  val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    // 周期性/间断性
    val data = stream.assignAscendingTimestamps(_.callTime).assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
        // 最长延迟10秒
        new BoundedOutOfOrdernessGenerator(10 * 1000L)
      }
    })

    // 有序的情况,为数据流中的元素分配时间戳，并定期创建水印以表示事件时间进度。 设置事件时间
    val result = data
      // 通话成功的记录
      .filter(_.callType.equals("success"))
      .keyBy(_.sid)
      // 每隔10秒统计最近20秒内，每个基站通话时间最长的一次通话记录的基站的id、通话时长、呼叫时间 (毫秒)，已经当前发生的时间范围(20秒)  窗口范围左闭右开 延迟的数据会丢掉
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
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




