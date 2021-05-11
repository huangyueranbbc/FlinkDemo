package com.hyr.flink.tableApiAndSQL.streaming

import com.hyr.flink.common.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/** *****************************************************************************
 *
 * @date 2021-05-11 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Table API
 ******************************************************************************/
object TableAPIWithDataStream {

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

    import org.apache.flink.table.api._
    // 将流转换为表 convert the DataStream into a Table with fields "id", "call_out", "call_in"
    val table: Table = tableEnv.fromDataStream(stream, $"id", $"call_out", $"call_in", $"call_type", $"call_time")

    table.printSchema()
    val tableResult: Table = table.filter($"call_type" === "fail")

    // 将表转换为流
    // Append Mode: 仅当动态 Table 仅通过INSERT更改进行修改时，才可以使用此模式，即，它仅是追加操作，并且之前输出的结果永远不会更新。
    // Retract Mode: 任何情形都可以使用此模式。它使用 boolean 值对 INSERT 和 DELETE 操作的数据进行标记。

    val dataStreamOnAppend: DataStream[(String, String, String, String, Long)] = tableEnv.toAppendStream[(String, String, String, String, Long)](tableResult)
    dataStreamOnAppend.print()

    // INSERT为true DELETE为false
    val tableResult1 = table.groupBy($"id").select($"id", $("id").count().as("log_count"))
    val dataStreamOnRetract: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](tableResult1)
    dataStreamOnRetract.print()

    // 启动流计算
    streamEnv.execute(this.getClass.getName)
  }

}
