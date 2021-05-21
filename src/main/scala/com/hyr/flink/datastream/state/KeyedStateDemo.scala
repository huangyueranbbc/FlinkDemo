package com.hyr.flink.datastream.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
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
 */
object KeyedStateDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[(Long, Long)] = streamEnv.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L)))

    dataStream.keyBy(_._1)
      .flatMap(new CountWindowAverage)
      .print()

    streamEnv.execute(this.getClass.getName)
  }

}

/**
 * 求窗口平均值
 */
class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Double)] {

  // ValueState<T> 保存一个可以更新和检索的值. 存放上一次输入的值
  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(input: (Long, Long), out: Collector[(Long, Double)]): Unit = {

    // access the state value
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    // update the count
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    // 如果获得了两条数据，则对两条数据求平均值，并清空状态
    if (newSum._1 >= 2) {
      println("input:" + input)
      out.collect((input._1, newSum._2.toDouble / newSum._1.toDouble))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}

