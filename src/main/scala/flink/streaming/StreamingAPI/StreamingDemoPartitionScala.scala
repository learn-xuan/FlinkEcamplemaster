package flink.streaming.StreamingAPI

import flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 实现自定义分区
  */
object StreamingDemoPartitionScala {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度，不设置默认电脑核数
    env.setParallelism(2)

    val text = env.addSource(new MyNoParallelSourceScala)

    // 把Long类型数据转化成Tuple类型
    val Tuple1_data = text.map(line => {
      Tuple1(line)
    })

    val partitionData = Tuple1_data.partitionCustom(new MyPartitionScala, 0)

    val result = partitionData.map(line => {
      println("当前线程id：" + Thread.currentThread().getId + ", value:" + line)
      line._1
    })

    result.print()setParallelism(1)

    env.execute("import org.apache.flink.api.scala._")


  }

}
