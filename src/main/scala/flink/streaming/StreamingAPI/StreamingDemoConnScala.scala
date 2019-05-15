package flink.streaming.StreamingAPI

import flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * connect方法可以合并不同类型的两个数据源
  */
object StreamingDemoConnScala {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val test1 = env.addSource(new MyNoParallelSourceScala)
    val test2 = env.addSource(new MyNoParallelSourceScala)

    val test2_str = test2.map(_+"_str")

    val newtest = test1.connect(test2_str)

    val result = newtest.map(line=>{line},line2=>{line2})

    result.print().setParallelism(1)

    env.execute("StreamingDemoConnScala")

  }

}
