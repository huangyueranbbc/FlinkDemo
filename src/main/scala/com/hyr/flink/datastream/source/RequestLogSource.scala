package com.hyr.flink.datastream.source

import com.hyr.flink.common.RequestLog
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.UUID
import scala.util.Random

/**
 *
 * @date 2021-05-52 1:59 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 自定义的请求日志Source,每秒钟生成COUNT条接口请求日志
 */
class RequestLogSource(count: Int) extends SourceFunction[RequestLog] {

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
  override def run(sourceContext: SourceFunction.SourceContext[RequestLog]): Unit = {
    val r = new Random()
    val ips = Array("192.1.168.150", "192.1.168.160", "192.1.168.170", "192.1.168.180")
    val vistors = Array("S101", "S201", "S301", "S401")
    val params = Array("{cust_code}", "{trd_date}", "{inst_code}", "{account}")
    val apis = Array("{getCustInfo}", "{getAsset}", "{getProductInfo}", "{getAccountInfo}")
    while (isStop) {
      1.to(count).map(_ => {
        val index1 = r.nextInt(4)
        val index2 = r.nextInt(4)
        val ip = ips(index1)
        val vistor = vistors(index1)
        val param = params(index2)
        val api = apis(index2)
        val costTime1 = r.nextInt(1600)
        val costTime2 = r.nextInt(1600)
        val uuid = UUID.randomUUID().toString
        //生成一条数据
        val requestLog = RequestLog(ip, vistor, param, api, uuid, System.currentTimeMillis(), costTime1 + costTime2)
        requestLog
      }).foreach(sourceContext.collect) //发送数据到流
      Thread.sleep(1000) //每秒发送一次数据
    }
  }

  /**
   * 终止数据流
   */
  override def cancel(): Unit = {
    isStop = false
  }

}
