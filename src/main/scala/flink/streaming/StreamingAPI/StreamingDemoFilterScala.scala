package flink.streaming.StreamingAPI

import flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Filter过滤掉不符合条件的数据
  */
object StreamingDemoFilterScala {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val test = env.addSource(new MyNoParallelSourceScala)

    val data = test.map(line => {
      println("过滤前的数据：" + line)
      line
    }).filter(data=>{
      data%2==0
    }).timeWindowAll(Time.seconds(2)).sum(0)

    data.print().setParallelism(1)

    env.execute("StreamingDemoFilterScala")

  }

}
