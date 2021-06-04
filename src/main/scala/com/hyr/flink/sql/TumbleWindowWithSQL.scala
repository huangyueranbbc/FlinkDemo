package com.hyr.flink.sql

import java.time.ZoneOffset
import com.hyr.flink.common.StationLog
import com.hyr.flink.common.watermarkgenerator.BoundedOutOfOrdernessGenerator
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 *
 * @date 2021-05-19 1:57 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: 通过SQL实现滚动时间窗口和水位线
 */
object TumbleWindowWithSQL {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    // 自定义web端口
    conf.setInteger(RestOptions.PORT, 9000)
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 每 1000ms 开始一次 checkpoint
    streamEnv.enableCheckpointing(1000)

    // 高级选项：

    /*
    设置checkpoint方式,如无配置，则默认使用MemoryStateBackend(data in heap memory / checkpoints to JobManager。它使用 JobManager 的堆保存状态快照。)
    ● RocksDBStateBackend	本地磁盘（tmp dir）	分布式文件系统	全量 / 增量 注:需引入第三方依赖
        支持大于内存大小的状态
        经验法则：比基于堆的后端慢10倍
    ● FsStateBackend	JVM Heap	分布式文件系统	全量
        快速，需要大的堆内存
        受限制于 GC
    ● MemoryStateBackend	JVM Heap	JobManager JVM Heap	全量
        适用于小状态（本地）的测试和实验
     */
    streamEnv.setStateBackend(new MemoryStateBackend())

    /*
    ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION 在取消拥有的作业时，所有检查点状态（元数据和实际程序状态）都将被删除
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION 当您取消拥有作业时，将保留所有检查点状态。下次启动时可以恢复当前状态进度。
     */
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置模式为精确一次 (这是默认值推荐值)
    // 为了实现端到端的精确一次，以便 sources 中的每个事件都仅精确一次对 sinks 生效，必须满足以下条件：
    // 1.你的 sources 必须是可重放的，并且
    // 2.你的 sinks 必须是事务性的（或幂等的）
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确认 checkpoints 之间的时间会进行 500 ms
    streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    streamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置可容忍的检查点失败数，默认值为0，这表示我们不容忍任何检查点失败。 整数最大值(意味着无限制）表示允许如果 task 的 checkpoint 发生错误，会阻止 task 失败。，作业管理器不会使整个作业失败，无论它收到多少个拒绝的检查点。
    streamEnv.getCheckpointConfig.setTolerableCheckpointFailureNumber(Int.MaxValue)
    // 同一时间只允许一个 checkpoint 进行
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 通过flink的原生的方式创建setting 实时
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    // 创建table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    import org.apache.flink.streaming.api.scala._


    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
      // val stream: DataStream[StationLog] = streamEnv.socketTextStream("127.0.0.1", 8888)
      //      .map(line => {
      //        val arr = line.split(",")
      //        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      //      })
      .assignAscendingTimestamps(_.callTime)
      // 水位线
      .assignTimestampsAndWatermarks(new WatermarkStrategy[StationLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          // 最长延迟10秒
          new BoundedOutOfOrdernessGenerator(10 * 1000L)
        }
      })

    import org.apache.flink.table.api._
    // 注册表,callTime为EventTime
    tableEnv.getConfig.setLocalTimeZone(ZoneOffset.ofHours(0))
    tableEnv.createTemporaryView("t_station", stream, $"sid", $"callOut", $"callIn", $"callType", $"callTime".rowtime, $"duration")

    val tableResult: Table = tableEnv.sqlQuery("SELECT sid,sum(duration) AS duration_sum"
      + ",TUMBLE_START(callTime, INTERVAL '5' SECOND) AS window_start"
      + ",TUMBLE_END(callTime, INTERVAL '5' SECOND) AS window_end "
      + "FROM t_station WHERE callType = 'success' "
      + "GROUP BY TUMBLE(callTime,INTERVAL '5' SECOND),sid")

    tableEnv.toRetractStream[Row](tableResult)
      .filter(_._1 == true)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
