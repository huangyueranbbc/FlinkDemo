package com.hyr.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/** *****************************************************************************
 *
 * @date 2021-03-08 9:03 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Flink批计算WordCount演示 有界流
 ******************************************************************************/
object FlinkBatchWordCount {

  def main(args: Array[String]): Unit = {
    // 1、初始化批计算环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3) // 设置默认并行度 会被算子级别的并行度覆盖

    // 2、导入隐式转换
    import org.apache.flink.api.scala._

    // 3、读取数据,读取socket流中的数据
    val dataSet: DataSet[String] = env.readTextFile("word")

    // 4、转换和处理数据
    val counts: AggregateDataSet[(String, Int)] = dataSet.flatMap {
      _.toLowerCase.split(" ").filter {
        _.nonEmpty
      }
    }.map((_, 1))
      .groupBy(0) // 分组算子 注:也可以传数字,0或者1表示下标
      .sum(1) // 聚合累加算子


    // 4、输出结果
    counts.print()
  }

}
