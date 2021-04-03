package com.hyr.flink.datastream.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/** *****************************************************************************
 *
 * @date 2021-03-13 4:35 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: kafka sink
 ******************************************************************************/
object KafkaSink {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("server3", 8888)

    //计算
    val words: DataStream[String] = stream.flatMap(_.split(" "))


    val properties = new Properties
    properties.setProperty("bootstrap.servers", "10.2.98.128:9092")

    // 将计算结果写入到KafkaSink中
    // kafka-topics --create --zookeeper 10.2.98.128:2181  --topic my-topic --partitions 1 --replication-factor 1
    val myProducer = new FlinkKafkaProducer[String](
      "my-topic", // 目标 topic
      new SimpleStringSchema(), // 序列化 schema
      properties) // producer 配置

    words.addSink(myProducer)
    // kafka-console-consumer --bootstrap-server 10.2.98.128:9092 --from-beginning --topic my-topic

    streamEnv.execute(this.getClass.getName)
  }

}
