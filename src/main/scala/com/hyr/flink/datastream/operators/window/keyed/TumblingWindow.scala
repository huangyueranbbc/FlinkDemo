package com.hyr.flink.datastream.operators.window.keyed

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/** *****************************************************************************
 *
 * @date 2021-03-30 9:34 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 滚动窗口
 *              stream.keyBy(...) // 是Keyed类型数据集
 *               .window(...) //指定窗口分配器类型
 *               [.trigger(...)] //指定触发器类型（可选）
 *               [.evictor(...)] //指定evictor或者不指定（可选）,用于剔除数据
 *               [.allowedLateness(...)] //指定是否延迟处理数据（可选）
 *               [.sideOutputLateData(...)] //指定Output Lag（可选）
 *               .reduce/aggregate/fold/apply() //指定窗口计算函数
 *               [.getSideOutput(...)] //根据Tag输出数据（可选）
 * *****************************************************************************/

object TumblingWindow {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

    // 统计5秒窗口内输入单词的出现次数
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
      // 设置间隔5秒的滚动窗口
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // TODO TumblingEventTimeWindows

    val result = windowData.sum(1)

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
