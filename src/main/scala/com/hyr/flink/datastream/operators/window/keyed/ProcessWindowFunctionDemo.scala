package com.hyr.flink.datastream.operators.window.keyed

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * @date 2021-03-30 8:02 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: ProcessWindowFunction全量窗口函数
 */
object ProcessWindowFunctionDemo {

  private val log: Logger = LoggerFactory.getLogger(ProcessWindowFunctionDemo.getClass)


  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    // 统计5秒内每个sid的总通话时间
    val result: DataStream[(String, Long)] = stream
      .keyBy(_.sid)
      // 设置间隔5秒的滚动窗口
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // TODO TumblingEventTimeWindows
      .process(new MyCustomProcessWindowFunction())

    result.print()

    streamEnv.execute(this.getClass.getName)
  }


  class MyCustomProcessWindowFunction extends ProcessWindowFunction[StationLog, (String, Long), String, TimeWindow] {

    /**
     * 窗口结束时，窗口内的每个分区执行此方法
     */
    override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[(String, Long)]): Unit = {
      log.info("MyCustomProcessWindowFunction.process! key:{}, elements.size:{}", key, elements.size)
      // 整个窗口的所有数据都保存在elements:Iterable迭代器中
      // 总通话时长
      var durationSum: Long = 0L
      for (elem <- elements) {
        // 窗口结束时间相同的为同一个窗口的数据
        log.info("context.window:{}, get elem:{}", context.window.getEnd, elem)
        durationSum += elem.duration
      }
      out.collect((key, durationSum))
    }
  }

}
