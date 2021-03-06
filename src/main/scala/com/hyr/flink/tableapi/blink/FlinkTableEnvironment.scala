package com.hyr.flink.tableapi.blink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 *
 * @date 2021-04-23 1:49 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: blink方式创建
 */
object FlinkTableEnvironment {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    // 通过flink的原生的方式创建setting 批量
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv = StreamTableEnvironment.create(streamEnv, settings)


  }

}
