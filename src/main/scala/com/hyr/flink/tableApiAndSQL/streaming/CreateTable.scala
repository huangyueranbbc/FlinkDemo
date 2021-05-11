package com.hyr.flink.tableApiAndSQL.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableResult}
import org.apache.flink.table.descriptors.Schema

/** *****************************************************************************
 *
 * @date 2021-05-12 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description:
 ******************************************************************************/
object CreateTable {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    // 创建表
    val sinkDDL =
      """create table station(
                            sid varchar,
                            callOut varchar,
                            callIn varchar,
                            callType varchar,
                            callTime bigint,
                            duration bigint
                          ) with (
                            'connector.type' = 'filesystem',
                            'format.type' = 'csv',
                            'connector.path' = 'station.csv'
                          )"""
    val createTableResult = tableEnv.executeSql(sinkDDL)
    createTableResult.print()

    // 也可使用以下的方法导入数据
    // 注册表
    val schema = new Schema()
      .field("sid", DataTypes.STRING())
      .field("callOut", DataTypes.STRING())
      .field("callIn", DataTypes.STRING())
      .field("callType", DataTypes.STRING())
      .field("callTime", DataTypes.BIGINT())
      .field("duration", DataTypes.BIGINT())
    //    tableEnv.connect(new FileSystem().path("station.csv"))
    //      .withFormat(new Csv())
    //      .withSchema(schema)
    //      .inAppendMode()
    //      .createTemporaryTable("station")

    tableEnv.executeSql("show tables")
    val result: TableResult = tableEnv.executeSql("select * from station where duration > 0")
    result.print()
  }

}
