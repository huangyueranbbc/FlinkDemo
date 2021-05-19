package com.hyr.flink.sql

import java.time.ZoneOffset

import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/** *****************************************************************************
 *
 * @date 2021-05-19 3:58 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 通过SQL实现滑动时间窗口和水位线
 ******************************************************************************/
object HopWindowWithSQL {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    // 自定义web端口
    conf.setInteger(RestOptions.PORT, 9000)
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    import org.apache.flink.streaming.api.scala._


    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
      // val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      //      .map(line => {
      //        val arr = line.split(",")
      //        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      //      })
      .assignAscendingTimestamps(_.callTime)
      // 水位线
      .assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          // 最长延迟10秒
          new BoundedOutOfOrdernessGenerator(10 * 1000L)
        }
      })

    import org.apache.flink.table.api._
    // 注册表,callTime为EventTime
    tableEnv.getConfig.setLocalTimeZone(ZoneOffset.ofHours(0))
    tableEnv.createTemporaryView("t_station", stream, $"sid", $"callOut", $"callIn", $"callType", $"callTime".rowtime, $"duration")

    val tableResult: Table = tableEnv.sqlQuery("SELECT sid,sum(duration) AS duration_sum"
      + ",HOP_START(callTime,INTERVAL '5' SECOND,INTERVAL '10' SECOND) AS window_start"
      + ",HOP_END(callTime,INTERVAL '5' SECOND,INTERVAL '10' SECOND) AS window_end "
      + "FROM t_station WHERE callType = 'success' "
      + "GROUP BY HOP(callTime,INTERVAL '5' SECOND,INTERVAL '10' SECOND),sid") // 每隔5秒统计宽度为10秒的窗口

    tableEnv.toRetractStream[Row](tableResult)
      .filter(_._1 == true)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
