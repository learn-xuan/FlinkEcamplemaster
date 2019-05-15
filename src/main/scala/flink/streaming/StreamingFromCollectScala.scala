package flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingFromCollectScala {

  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      import org.apache.flink.api.scala._

      val data = List(23,12,45,67)

      val test = env.fromCollection(data)

      val num = test.map(_+1)

      num.print().setParallelism(1)

      env.execute("StreamingFromCollectScala")

  }

}
