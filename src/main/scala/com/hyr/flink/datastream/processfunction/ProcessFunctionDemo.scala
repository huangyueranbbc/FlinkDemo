package com.hyr.flink.datastream.processfunction

import com.hyr.flink.common.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/** *****************************************************************************
 *
 * @date 2021-03-24 8:59 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: ProcessFunction 是一个低层次的流处理操作，允许返回所有 Stream 的基础构建模块:
 *               ◆ 访问 Event事件 本身数据（比如：Event 的时间，Event 的当前 Key 等）
 *               ◆ 管理状态 State（仅在 Keyed Stream 中）
 *               ◆ 管理定时器 Timer（包括：注册定时器，删除定时器等）
 * *****************************************************************************/
object ProcessFunctionDemo {

  // 监控每一个手机，如果在 5 秒内呼叫它的通话都是失败的，发出警告信息。
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val result = stream.keyBy(_.callInt).process(new CallInFailMonitorProcess)
    result.print()

    streamEnv.execute(this.getClass.getName)
  }
}

/**
 * 监控被叫失败
 */
class CallInFailMonitorProcess extends KeyedProcessFunction[String, StationLog, String] {

  // 创建事件状态
  lazy val timesState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

  /**
   * @param value The input value.
   * @param ctx   A { @link Context} that allows querying the timestamp of the element and getting a
   *              { @link TimerService} for registering timers and querying the time. The context is only
   *              valid during the invocation of this method, do not store it.
   * @param out   The collector for returning result values.
   * @throws Exception This method may throw exceptions. Throwing an exception will cause the
   *                   operation to fail and may trigger recovery.
   */
  override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
    // 从状态获取上一次状态失败的次数
    val time = timesState.value()

    if (time == 0 && value.callType.equals("fail")) {
      // 第一次发现呼叫失败
      //获取当前系统时间，并注册定时器
      val nowTime = ctx.timerService().currentProcessingTime()
      // 定时器在5秒后触发
      val onTime = nowTime + 5 * 1000L
      ctx.timerService().registerProcessingTimeTimer(onTime)
      // 保存当前时间
      timesState.update(onTime)
    }

    if (time != 0 && !value.callType.equals("fail")) {
      // 5秒内有一次失败，且有一次呼叫成功
      ctx.timerService().deleteProcessingTimeTimer(time)
      // 清空状态中的时间
      timesState.clear()
    }
  }

  // 5秒内所有的呼叫都失败了，触发定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
    val alertMessage = "触发告警时间:" + timestamp + " 手机号:" + ctx.getCurrentKey
    out.collect(alertMessage)
    timesState.clear()
  }

}

