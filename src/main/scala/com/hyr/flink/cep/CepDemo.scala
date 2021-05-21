package com.hyr.flink.cep

import com.hyr.flink.common.RequestLog
import com.hyr.flink.datastream.source.RequestLogSource
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util

/**
 * @date 2021-05-21 1:28 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 如果某个接口调用时间在1分钟内连续3次超过3秒,则告警
 */
object CepDemo {

  private val log: Logger = LoggerFactory.getLogger(CepDemo.getClass)

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    // 自定义web端口
    conf.setInteger(RestOptions.PORT, 9000)
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    streamEnv.setParallelism(3)

    import org.apache.flink.streaming.api.scala._

    // 1.读取数据
    val dataStream: DataStream[RequestLog] = streamEnv.addSource(new RequestLogSource(100))
      .assignAscendingTimestamps(_.requestTime) // 精确到毫秒

    // 2.定时模式
    //    next()，指定严格连续，
    //    followedBy()，指定松散连续，
    //    followedByAny()，指定不确定的松散连续。
    val pattern = Pattern.begin[RequestLog]("timeout1").where(_.costTime > 3000) // 耗时超过3秒
      .followedBy("timeout2").where(_.costTime > 3000) // 指定松散连续
      .followedBy("timeout3").where(_.costTime > 3000)
      .within(Time.seconds(60)) // 默认基于事件时间

    // 3.检测模式
    val patternStream: PatternStream[RequestLog] = CEP.pattern(dataStream.keyBy(_.vistor).keyBy(_.api), pattern) // 根据接口名称分组

    // 4.选择结果并输出
    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[RequestLog, String] {
      override def select(map: util.Map[String, util.List[RequestLog]]): String = {
        val valueIterator = map.entrySet().iterator()
        val event1: RequestLog = valueIterator.next().getValue.iterator().next()
        val event2: RequestLog = valueIterator.next().getValue.iterator().next()
        val event3: RequestLog = valueIterator.next().getValue.iterator().next()
        log.info("event1:{} ,event2:{} ,event3:{}", event1, event2, event3)

        s"api request timeout alter!   visitor:${event1.vistor} ip:${event1.ip} api:${event1.api} " +
          s"requestTime1: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(event1.requestTime)} " +
          s"requestTime2: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(event2.requestTime)} " +
          s"requestTime3: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(event3.requestTime)} " +
          s"costTime1:${event1.costTime} " +
          s"costTime2:${event2.costTime} " +
          s"costTime3:${event3.costTime} "
      }
    })

    // 告警
    result.print()

    streamEnv.execute(this.getClass.getName)
  }

}
