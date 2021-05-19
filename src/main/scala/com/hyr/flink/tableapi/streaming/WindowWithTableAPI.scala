package com.hyr.flink.tableapi.streaming

import java.time.ZoneOffset

import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig}
import org.apache.flink.types.Row

/** *****************************************************************************
 *
 * @date 2021-05-13 9:57 下午
 * @author: <a href=mailto:huangyr@com>huangyr</a>
 * @Description: TableAPI的时间窗口和水位线
 * *****************************************************************************/
object WindowWithTableAPI {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    // 自定义web端口
    conf.setInteger(RestOptions.PORT, 9000)
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 多并行度会自动对齐WaterMark，取最小的WaterMark。避免干扰，将并行度设为1
    streamEnv.setParallelism(8)
    // 周期性的引入WaterMark 间隔100毫秒
    streamEnv.getConfig.setAutoWatermarkInterval(1000)
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)
    val tableConfig: TableConfig = tableEnv.getConfig
    tableConfig.setLocalTimeZone(ZoneOffset.ofHours(8))
    tableConfig.setNullCheck(true)

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
    // 处理时间属性可以在 schema 定义的时候用 .proctime 后缀来定义。
    // 事件时间属性可以用 .rowtime 后缀在定义 DataStream schema 的时候来定义。
    val table: Table = tableEnv.fromDataStream(stream, $"sid", $"callOut", $"callIn", $"callType", $"callTime".rowtime(), $"duration")
    table.printSchema()

    // 开窗口
    // val windowedTable = table.window(Slide over 10.second every 5.second on $"callTime" as "callTimeWindow") // 滑动窗口

    // val windowedTable = table.window(Tumble.over(5.second).on($"callTime").as("callTimeWindow")) // 滚动窗口
    val windowedTable: GroupWindowedTable = table.window(Tumble over 5.second on $"callTime" as "callTimeWindow") // 滚动窗口
    val tableResult: Table = windowedTable.groupBy($"callTimeWindow", $"sid")
      .select($"sid", $"callTimeWindow".start, $"callTimeWindow".end, $"sid".count)

    val result: DataStream[(Boolean, Row)] = tableEnv.toRetractStream(tableResult)
    result.filter(_._1 == true)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
