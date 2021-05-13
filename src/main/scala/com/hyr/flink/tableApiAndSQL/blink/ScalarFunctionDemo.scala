package com.hyr.flink.tableApiAndSQL.blink

import com.hyr.flink.common.customfunction.HashFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/** *****************************************************************************
 *
 * @date 2021-05-12 8:58 上午
 * @author: <a href=mailto:huangyr@com>huangyr</a>
 * @Description: 标量函数 自定义标量函数可以把 0 到多个标量值映射成 1 个标量值
 * *****************************************************************************/
object ScalarFunctionDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 通过blink的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._
    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)
    tableEnv.createTemporaryView("line", stream, $"str_value")

    // 注册函数
    tableEnv.createTemporarySystemFunction("HashFunction", classOf[HashFunction])

    // 在 Table API 里调用注册好的函数
    val table1: Table = tableEnv.from("line").select(call("HashFunction", $"str_value"))
    // 注: 新的类型转换需要使用blink进行支持
    // The new type inference for functions is only supported in the Blink planner. Falling back to legacy type inference for function 'class com.hyr.flink.common.customfunction.HashFunction'.
    tableEnv.toAppendStream[Row](table1).print()

    // 在 SQL 里调用注册好的函数
    val table2: Table = tableEnv.sqlQuery("SELECT HashFunction(str_value) FROM line")
    tableEnv.toAppendStream[Row](table2).print()

    streamEnv.execute(this.getClass.getName)
  }

}
