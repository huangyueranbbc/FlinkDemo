package com.hyr.flink.datastream.state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * @date 2021-03-29 3:34 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: checkpoint，由JobManager触发。JobManager会根据间隔不断的在数据流中插入barrier,当算子收到barrier后，会执行checkpoint。
 *               Savepoint,通过用户手工命令触发。
 *               flink run -s checkpointdir 从指定checkpoint目录恢复程序
 *               为了让状态容错，Flink 需要为状态添加 checkpoint（检查点）。Checkpoint 使得 Flink 能够恢复状态和在流中的位置，从而向应用提供和无故障执行时一样的语义。
 *               当 checkpoint coordinator（job manager 的一部分）指示 task manager 开始 checkpoint 时，它会让所有 sources 记录它们的偏移量，
 *               并将编号的 checkpoint barriers 插入到它们的流中。这些 barriers 流经 job graph，标注每个 checkpoint 前后的流部分。
 *               当 job graph 中的每个 operator 接收到 barriers 时，它就会记录下其状态checkpoint。
 *               拥有两个输入流的 Operators（例如 CoProcessFunction）会执行 barrier 对齐（barrier alignment） 以便当前快照能够包含消费两个输入流 barrier 之前（但不超过）的所有 events 而产生的状态。
 */
object StateCheckpointing {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

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
    streamEnv.setStateBackend(new FsStateBackend("file:///Users/huangyueran/ideaworkspaces/myworkspaces/FlinkDemo/checkpoint"))

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

    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("127.0.0.1", 8888)
      // 指定算子id号
      .uid("source")

    val counts: DataStream[(String, Int)] = stream.flatMap {
      _.toLowerCase.split(" ").filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .uid("map")
      .keyBy(_._1) // 分组算子 注:也可以传数字,0或者1表示下标
      .sum(1) // 聚合累加算子


    // 4、输出结果
    counts.print().setParallelism(1) // 设置算子级别并行度

    // 5、启动流计算程序
    streamEnv.execute(this.getClass.getName)
  }
}
