package com.hyr.flink.datastream.source

import com.hyr.flink.common.StationLog
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/** *****************************************************************************
 *
 * @date 2021-03-13 4:59 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 自定义的Source,需求：每隔两秒钟，生成10条随机基站通话日志数据
 * *****************************************************************************/
object CustomSource {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    stream.print()

    streamEnv.execute(this.getClass.getName)
  }

}

class MyCustomSource extends SourceFunction[StationLog] {

  /**
   * 是否终止数据流
   */
  var isStop = true;

  /**
   * 主要方法,启动一个source，并且从source中返回数据
   * 如果run方式终止，则数据流终止
   *
   * @param sourceContext 数据源内容
   */
  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {
    val r = new Random()
    val types = Array("fail", "basy", "barring", "success")
    while (isStop) {
      1.to(10).map(_ => {
        val callOut = "1860000%04d".format(r.nextInt(10000)) //主叫号码
        val callIn = "1890000%04d".format(r.nextInt(10000)) //被叫号码
        //生成一条数据 可能会有1分钟的延迟
        StationLog("station_" + r.nextInt(10), callOut, callIn, types(r.nextInt(4)), System.currentTimeMillis() - Random.nextInt(1000  * 60) + Random.nextInt(1000 * 10), r.nextInt(20))
      }).foreach(sourceContext.collect) //发送数据到流
      Thread.sleep(2000) //每隔2秒发送一次数据
    }
  }

  /**
   * 终止数据流
   */
  override def cancel(): Unit = {
    isStop = false
  }

}
