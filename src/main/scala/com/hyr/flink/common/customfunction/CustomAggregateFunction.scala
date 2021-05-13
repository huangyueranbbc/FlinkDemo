package com.hyr.flink.common.customfunction

import org.apache.flink.table.functions.TableAggregateFunction


/** *****************************************************************************
 *
 * @date 2021-05-12 10:24 上午 
 * @author: <a href=mailto:huangyr@>huangyr</a>
 * @Description:
 * *****************************************************************************/
class CustomAggregateFunction extends TableAggregateFunction{
  override def createAccumulator(): Nothing = ???

}
