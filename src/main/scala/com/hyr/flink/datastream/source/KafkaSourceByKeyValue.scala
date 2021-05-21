package com.hyr.flink.datastream.source

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.flink.streaming.api.scala._

/**
 *
 * @date 2021-03-13 4:09 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: kafka source key-value
 *               https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/connectors/
 */
object KafkaSourceByKeyValue {

  //自定义一个类，从Kafka中读取键值对的数据
  class MyKafkaReader extends KafkaDeserializationSchema[(String, String)] {
    //是否流结束
    override def isEndOfStream(nextElement: (String, String)): Boolean = {
      false
    }

    //反序列化
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
      if (record != null) {
        var key = "null"
        var value = "null"
        if (record.key() != null) {
          key = new String(record.key(), "UTF-8")
        }
        if (record.value() != null) { //从Kafka记录中得到Value
          value = new String(record.value(), "UTF-8")
        }
        (key, value)
      } else { //数据为空
        ("null", "null")
      }
    }

    //指定类型
    override def getProducedType: TypeInformation[(String, String)] = {
      createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
    }
  }

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.2.98.128:9092")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("auto.offset.reset", "earliest")

    val kafkaComsumer = new FlinkKafkaConsumer[(String, String)]("kbssusertopic", new MyKafkaReader(), properties)
    val stream = streamEnv.addSource(kafkaComsumer)

    stream.print()

    streamEnv.execute()
  }

}
