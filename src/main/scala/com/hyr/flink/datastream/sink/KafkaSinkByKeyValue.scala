package com.hyr.flink.datastream.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
 *
 * @date 2021-03-13 4:48 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: kafka key-value sink
 */
object KafkaSinkByKeyValue {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("server3", 8888)

    //计算
    val counts: DataStream[(String, Int)] = stream.flatMap {
      _.toLowerCase.split(" ").filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(_._1) // 分组算子 注:也可以传数字,0或者1表示下标
      .sum(1) // 聚合累加算子

    //创建连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers", "10.2.98.128:9092")
    props.setProperty("print.key", "true")

    //创建一个Kafka的sink
    val kafkaSink = new FlinkKafkaProducer[(String, Int)](
      "my-topic",
      new KafkaSerializationSchema[(String, Int)] { //自定义的匿名内部类
        override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("my-topic", element._1.getBytes, (element._2 + "").getBytes)
        }

      },
      props, //连接Kafka的数学
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE //精确一次
    )

    counts.addSink(kafkaSink)

    streamEnv.execute(this.getClass.getName)
    //--property print.key=true Kafka的命令加一个参数
  }

}
