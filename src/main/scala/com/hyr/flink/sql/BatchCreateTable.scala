package com.hyr.flink.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

/** *****************************************************************************
 *
 * @date 2021-05-12 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 批量离线方式SQL
 * *****************************************************************************/
object BatchCreateTable {

  def main(args: Array[String]): Unit = {
    val streamEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 创建table环境
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(streamEnv)

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

    tableEnv.executeSql("show tables").print()
    val result: TableResult = tableEnv.executeSql("select sid,count(1) as sid_count from station where callType = 'success' group by sid")
    result.print()
  }

}
