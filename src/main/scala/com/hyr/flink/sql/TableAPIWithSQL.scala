package com.hyr.flink.sql

import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 *
 * @date 2021-05-12 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: TableAPI和SQL混用
 */
object TableAPIWithSQL {

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
    //读取数据源
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
      .assignAscendingTimestamps(_.callTime)
      // 水位线
      .assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          // 最长延迟10秒
          new BoundedOutOfOrdernessGenerator(10 * 1000L)
        }
      })

    import org.apache.flink.table.api._
    val table: Table = tableEnv.fromDataStream(stream, $"sid", $"callOut", $"callIn", $"callType", $"callTime", $"duration")

    val result: Table = tableEnv.sqlQuery(s"select sid,max(duration) as max_duration from $table where callType = 'success' group by sid")

    tableEnv.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
