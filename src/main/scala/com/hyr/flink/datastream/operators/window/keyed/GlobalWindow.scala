package com.hyr.flink.datastream.operators.window.keyed

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

import scala.collection.mutable

/**
 *
 * @date 2021-03-30 2:22 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 全局窗口
 */
object GlobalWindow {


  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

    // 每隔5秒，统计近10秒内输入单词的次数
    val windowData: WindowedStream[(String, Int), String, GlobalWindow] = stream
      .flatMap {
        _.toLowerCase.split(" ").filter {
          _.nonEmpty
        }
      }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      // 设置间隔5秒的滑动窗口
      .window(GlobalWindows.create())
      .trigger(new ProcessTimeTrigger)

    val result: DataStream[(String, Int)] = windowData.sum(1)

    result.print()

    streamEnv.execute(this.getClass.getName)
  }

  class ProcessTimeTrigger extends Trigger[(String, Int), GlobalWindow] {

    private val flag: mutable.Map[String, Int] = mutable.Map[String, Int]()

    /**
     * 当某个单词在全局窗口内出现了10次，触发
     */
    override def onElement(element: (String, Int), timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (!flag.contains(element._1)) {
        flag.put(element._1, 0)
      }
      if (flag(element._1) >= 10) {
        flag.put(element._1, 0)
        TriggerResult.FIRE_AND_PURGE
      } else {
        flag.put(element._1, flag(element._1) + 1)
        println("onElement : " + element)
        println("flag : " + flag)
        TriggerResult.CONTINUE
      }
    }

    /**
     * 当注册的事件时间计时器触发时，将调用此方法。
     */
    override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("com.hyr.flink.datastream.operators.window.keyed.GlobalWindow.ProcessTimeTrigger.onProcessingTime!")
      TriggerResult.CONTINUE
    }

    /**
     * 当注册的处理时间计时器触发时，将调用此方法。
     */
    override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("com.hyr.flink.datastream.operators.window.keyed.GlobalWindow.ProcessTimeTrigger.onEventTime!")
      TriggerResult.CONTINUE
    }

    /**
     * 执行删除相应窗口时所需的任何操作。(一般是删除定义的状态、定时器等)
     */
    override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
      println("com.hyr.flink.datastream.operators.window.keyed.GlobalWindow.ProcessTimeTrigger.clear!")
      ctx.deleteEventTimeTimer(window.maxTimestamp())
    }

    /**
     * 与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态。
     */
    override def canMerge: Boolean = {
      true
    }
  }

}
