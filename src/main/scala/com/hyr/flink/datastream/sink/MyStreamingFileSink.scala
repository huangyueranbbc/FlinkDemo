package com.hyr.flink.datastream.sink

import java.util.concurrent.TimeUnit

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

/**
 *
 * @date 2021-03-13 5:14 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 流处理的文件sink
 */
object MyStreamingFileSink {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    val sink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat(new Path("hdfs://server1:8020/user/flink/stationlog"), new SimpleStringEncoder[StationLog]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder() //  默认滚动策略,默认一小时一个目录(分桶)
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()

    stream.addSink(sink)

    streamEnv.execute()
  }

}
