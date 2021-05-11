package com.hyr.flink.tableApiAndSQL.streaming

import com.hyr.flink.common.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}

/** *****************************************************************************
 *
 * @date 2021-05-11 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 流转换为Table
 ******************************************************************************/
object CreateTableByDataStream {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    // 建表
    tableEnv.createTemporaryView("station", stream)

    // 获取table对象,使用table的api
    val stationTable: Table = tableEnv.from("station")
    stationTable.printSchema()

    // 查询临时表
    val result: TableResult = tableEnv.executeSql("select * from station where duration > 0")
    result.print()

    // 启动流计算
    streamEnv.execute(this.getClass.getName)
  }

}
