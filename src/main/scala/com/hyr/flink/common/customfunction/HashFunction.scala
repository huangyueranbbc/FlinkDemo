package com.hyr.flink.common.customfunction

import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.functions.ScalarFunction

/** *****************************************************************************
 *
 * @date 2021-05-11 9:01 下午
 * @author: <a href=mailto:huangyr@com>huangyr</a>
 * @Description: 自定义标量函数
 * *****************************************************************************/
class HashFunction extends ScalarFunction {

  // 接受任意类型输入，返回 INT 型输出
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) o: AnyRef): Int = {
    o.hashCode();
  }
}
