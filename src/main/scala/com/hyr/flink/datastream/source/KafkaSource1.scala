package com.hyr.flink.datastream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/** *****************************************************************************
 *
 * @date 2021-03-13 4:09 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: kafka source 字符串
 *               https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/connectors/
 * *****************************************************************************/
object KafkaSource1 {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.2.98.128:9092")
    properties.setProperty("group.id", "hyr_test")

    import org.apache.flink.streaming.api.scala._
    val kafkaComsumer = new FlinkKafkaConsumer[String]("kbssusertopic", new SimpleStringSchema(), properties)
    kafkaComsumer.setStartFromEarliest()
    val stream = streamEnv.addSource(kafkaComsumer)

    stream.print()

    streamEnv.execute(this.getClass.getName)
  }

}
