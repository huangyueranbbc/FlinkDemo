package com.hyr.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

/**
 *
 * @date 2021-05-12 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: SQL
 */
object StreamingCreateTable {

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
    tableEnv.executeSql(sinkDDL)

    val result: Table = tableEnv.sqlQuery("select sid,max(duration) as max_duration from station where callType = 'success' group by sid")

    import org.apache.flink.streaming.api.scala._
    tableEnv.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
