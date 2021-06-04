package com.hyr.flink.iceberg

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/** *****************************************************************************
 * @date 2021-06-04 10:31 上午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: Apache Iceberg
 * **************************************************************************** */
object IcebergDemo {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    // 自定义web端口
    conf.setInteger(RestOptions.PORT, 9000)
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    streamEnv.setParallelism(1)
    val tenv = StreamTableEnvironment.create(streamEnv)

    // add hadoop config file
    tenv.executeSql("CREATE CATALOG hive_catalog WITH (\n  'type'='iceberg',\n  'catalog-type'='hive',\n  'uri'='thrift://server1:9083',\n  'clients'='5',\n  'property-version'='1',\n  'warehouse'='hdfs://server1:8020/user/hive/warehouse'\n)");

    tenv.useCatalog("hive_catalog");
    tenv.executeSql("show databases").print()
    tenv.useDatabase("iceberg_db")
    tenv.executeSql("show tables").print()

    tenv.executeSql("select id from test").print() // TODO iceberg和flink版本不兼容 等待后续iceberg版本修复
  }

}
