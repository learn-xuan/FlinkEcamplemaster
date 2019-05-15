package flink.streaming.custormSource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * 创建自定义的source，并行度只能为1
  *
  * 实现数字从1开始递增
  */
class MyNoParallelSourceScala extends SourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      sourceContext.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }


}
