package flink.streaming

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * wordCount单词计数设置checkpoint
  */
object StreamingWordCountChickenPointScala {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    // 设置检查点, 每隔1000ms进行启动一个检查点
//    env.enableCheckpointing(1000)
//    // 高级选项
//    // 设置模式为exactly-once （这是默认值）
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    // 确保检查点之间有至少500ms的间隔【checkpoint最小间隔】
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint超时时间】
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//    // 同一时间只允许进行1个检查点
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)





    val port = 9001

    val hostname = "192.168.200.147"

    env.setParallelism(1)

    val source = env.socketTextStream(hostname, port, '\n')

    // 将数据转化成tuple2格式
    val tuple2_text = source.flatMap(line=>line.split("\\s"))
      .map(Tuple2(_,1))

    val windowCount = tuple2_text.keyBy(0)
      .timeWindowAll(Time.seconds(3))
      .sum(1)

    windowCount.print().setParallelism(1)

    env.execute("StreamingWordCountChickenPointScala")

  }

}
