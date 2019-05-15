package flink.streaming.StreamingAPI

import flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * union,合并两个相同数据类型的数据
  */
object StreamingunionScala {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val test1 = env.addSource(new MyNoParallelSourceScala)
    val test2 = env.addSource(new MyNoParallelSourceScala)

    val union_text = test1.union(test2)

    val sum = union_text.map(line => {
      println("接收到的数据：" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute("StreamingunionScala")


  }

}
