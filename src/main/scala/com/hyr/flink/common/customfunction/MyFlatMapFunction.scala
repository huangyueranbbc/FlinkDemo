package com.hyr.flink.common.customfunction

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


/**
 *
 * @date 2021-05-11 9:01 下午
 * @author: <a href=mailto:huangyr@com>huangyr</a>
 * @Description: 自定义Table函数
 */
class MyFlatMapFunction extends TableFunction[Row] {


  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }


  def eval(line: String): Unit = {
    line.split(" ").foreach(word => {
      val row = new Row(2)
      row.setField(0, word)
      row.setField(1, 1)
      collect(row)
    })
  }

}
