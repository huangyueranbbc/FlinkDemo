package com.hyr.flink.datastream.state

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** *****************************************************************************
 *
 * @date 2021-03-25 7:56 下午
 * @author: <a href=mailto:huangyr>huangyr</a>
 * @Description: keyed state
 *               ValueState<T>: This keeps a value that can be updated and retrieved (scoped to key of the input element as mentioned above, so there will possibly be one value for each key that the operation sees). The value can be set using update(T) and retrieved using T value().
 *
 *               ListState<T>: This keeps a list of elements. You can append elements and retrieve an Iterable over all currently stored elements. Elements are added using add(T) or addAll(List<T>), the Iterable can be retrieved using Iterable<T> get(). You can also override the existing list with update(List<T>)
 *
 *               ReducingState<T>: This keeps a single value that represents the aggregation of all values added to the state. The interface is similar to ListState but elements added using add(T) are reduced to an aggregate using a specified ReduceFunction.
 *
 *               AggregatingState<IN, OUT>: This keeps a single value that represents the aggregation of all values added to the state. Contrary to ReducingState, the aggregate type may be different from the type of elements that are added to the state. The interface is the same as for ListState but elements added using add(IN) are aggregated using a specified AggregateFunction.
 *
 *               MapState<UK, UV>: This keeps a list of mappings. You can put key-value pairs into the state and retrieve an Iterable over all currently stored mappings. Mappings are added using put(UK, UV) or putAll(Map<UK, UV>). The value associated with a user key can be retrieved using get(UK). The iterable views for mappings, keys and values can be retrieved using entries(), keys() and values() respectively. You can also use isEmpty() to check whether this map contains any key-value mappings.
 ******************************************************************************/
object KeyedWithStateDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[(String, Int)] = streamEnv.fromCollection(List(
      ("aa", 3),
      ("bb", 5),
      ("aa", 7),
      ("cc", 4),
      ("bb", 2)))

    // 根据Key求和
    dataStream.keyBy(_._1)
      .mapWithState((in: (String, Int), count: Option[Int]) => {
        count match {
          // Option 使用模式匹配初始有状态和无状态的情况
          case None => ((in._1, in._2), Some(in._2))
          case Some(lastCount) => ((in._1, lastCount + in._2), Some(lastCount + in._2))
        }
      })
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}
