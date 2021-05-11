package com.hyr.flink.tableApiAndSQL.streaming

import com.hyr.flink.common.customfunction.MyFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/** *****************************************************************************
 *
 * @date 2021-05-11 8:58 下午
 * @author: <a href=mailto:huangyr@com>huangyr</a>
 * @Description: 基于TableAPI实现WordCount
 ******************************************************************************/
object WordCountByCustomTableAPI {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

    import org.apache.flink.table.api._
    // 将流转换为表
    val table: Table = tableEnv.fromDataStream(stream, $"line")
    val myCustomFunction: MyFlatMapFunction = new MyFlatMapFunction

    val tableResult: Table = table.flatMap(myCustomFunction($"line"))
      .as("word", "count")
      .groupBy($"word")
      .select($"word", $("count").sum().as("word_count"))
    tableResult.printSchema()

    val resultDataStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream(tableResult)
    resultDataStream.print()

    streamEnv.execute(this.getClass.getName)
  }

}
