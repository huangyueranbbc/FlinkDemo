package com.hyr.flink.datastream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hyr.flink.common.StationLog
import com.hyr.flink.datastream.source.MyCustomSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/** *****************************************************************************
 *
 * @date 2021-03-18 1:38 下午
 * @author: <a href=mailto:@>huangyr</a>
 * @Description: 自定义的JDBCsink 随机生成StationLog对象，写入Mysql(t_station_log)
 *               create table t_station_log(
 *               sid varchar(255),
 *               call_out varchar(255),
 *               call_in varchar(255),
 *               call_type varchar(255),
 *               call_time varchar(255),
 *               duration varchar(255)
 *               )
 * *****************************************************************************/
object CustomJDBCSink {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomSource)

    //数据写入Mysql，所有需要创建一个自定义的sink
    stream.addSink(new MyCustomJDBCSink)

    streamEnv.execute(this.getClass.getName)
  }

}

class MyCustomJDBCSink extends RichSinkFunction[StationLog] {
  var conn: Connection = _
  var pst: PreparedStatement = _

  override def invoke(value: StationLog, context: SinkFunction.Context): Unit = {
    pst.setString(1, value.sid)
    pst.setString(2, value.callOut)
    pst.setString(3, value.callInt)
    pst.setString(4, value.callType)
    pst.setLong(5, value.callTime)
    pst.setLong(6, value.duration)
    pst.executeUpdate()
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://10.2.98.130/flink?useUnicode=true&characterEncoding=UTF-8&useSSL=false",  "", "")
    pst = conn.prepareStatement("insert into t_station_log (sid,call_out,call_in,call_type,call_time,duration) values (?,?,?,?,?,?)")
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }


}
