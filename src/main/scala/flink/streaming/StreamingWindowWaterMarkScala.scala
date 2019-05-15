package flink.streaming

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * flink 水印案例实现
  */
object StreamingWindowWaterMarkScala {
  def main(args: Array[String]): Unit = {

    val port = 9000
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    // 设置时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置并行度
    env.setParallelism(1)

    // 数据源，接收socket数据,数据格式：数据，时间戳
    val source = env.socketTextStream("",port ,'\n')

    // 对数据做处理，转换成TUple2
    val tupletext = source.map(line => {
      val strings = line.split(",")
      (strings(0), strings(1).toLong)
    })

    // 抽取时间戳生成watermark
    val waterMarkStream = tupletext.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 允许的最大乱序时间是10s

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
        * 定义生成watermark的逻辑
        * 默认100ms调用一次
        *
        * @return
        */
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      /**
        * 定义提取timestmp
        *
        * @param t
        * @param l
        * @return
        */
      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ",key:" + t._1 + ",eventtime:[" + t._2 + "|" + sdf.format(t._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")
        timestamp
      }
    })

    val window = waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3))) // 按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      .apply(new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {

        val keystr = key.toString
        val arrBuf = ArrayBuffer[Long]()
        val iterator = input.iterator
        while (iterator.hasNext) {
          val tuple2 = iterator.next()
          arrBuf.append(tuple2._2)
        }

        val str = arrBuf.toArray
        Sorting.quickSort(str)

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val result = keystr + "," + str.length + "," + sdf.format(str.head) + "," + sdf.format(str.last) + "," + sdf.format(window.getStart) + "," + sdf.format(window.getEnd)

        out.collect(result)
      }
    })

    window.print()

    env.execute("StreamingWindowWaterMarkScala")


  }
}
