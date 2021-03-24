package com.hyr.flink.datastream.operators

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/** *****************************************************************************
 *
 * @date 2021-03-23 8:38 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: Split对数据集按照条件进行拆分，Select根据标签选择数据集
 ******************************************************************************/
object SideOutPutOperator {


  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)
    // 根据通话成功和通话失败切分数据流
    val result: DataStream[StationLog] = dataStream.process(new StationProcess)

    result.getSideOutput(new OutputTag[StationLog]("success")).print()
    //result.getSideOutput(new OutputTag[StationLog]("fail")).print()

    streamEnv.execute(this.getClass.getName)
  }
}

class StationProcess() extends ProcessFunction[StationLog, StationLog] {
  // 定义侧输出流
  lazy val SUCCESS: OutputTag[StationLog] = new OutputTag[StationLog]("success")
  lazy val FAIL: OutputTag[StationLog] = new OutputTag[StationLog]("fail")

  override def processElement(stationLog: StationLog, context: ProcessFunction[StationLog, StationLog]#Context, collector: Collector[StationLog]): Unit = {
    stationLog.callType match {
      case "success" => context.output(SUCCESS, stationLog)
      case "fail" => context.output(FAIL, stationLog)
      case _ => println("no this type")
    }
  }
}
