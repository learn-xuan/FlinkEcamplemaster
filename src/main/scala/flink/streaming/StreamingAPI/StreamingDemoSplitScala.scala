package flink.streaming.StreamingAPI

import java.{lang, util}

import flink.streaming.custormSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * split切分数据的方法并给数据打上标签，用select选择需要的
  */
object StreamingDemoSplitScala {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.addSource(new MyNoParallelSourceScala)

    val splitStream = text.split(new OutputSelector[Long] {
      override def select(out: Long): lang.Iterable[String] = {
        val arrayList = new util.ArrayList[String]()
        if (out % 2 == 0) {
          arrayList.add("enve") // 偶数
        } else {
          arrayList.add("odd") // 奇数
        }
        arrayList
      }
    })

    val oddResult = splitStream.select("odd")

    oddResult.print().setParallelism(1)

    env.execute("import org.apache.flink.api.scala._")

  }

}
