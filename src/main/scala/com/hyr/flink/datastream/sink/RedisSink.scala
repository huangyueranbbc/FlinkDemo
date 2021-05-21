package com.hyr.flink.datastream.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 *
 * @date 2021-03-13 5:54 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description:
 */
object RedisSink {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //读取数据源
    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)

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
    //把结果写入Redis中
    //设置连接Redis的配置


    // val config: FlinkJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder().setMasterName("") // 哨兵模式

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setDatabase(0).setHost("10.2.98.133").setPort(6379).setPassword("").build()

    //设置Redis的Sink
    counts.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
      //设置redis的命令
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "t_wc")
      }

      //从数据中获取Key
      override def getKeyFromData(data: (String, Int)): String = {
        data._1
      }

      //从数据中获取Value
      override def getValueFromData(data: (String, Int)): String = {
        data._2 + ""
      }
    }))


    streamEnv.execute(this.getClass.getName)
  }

}
