package flink.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * kafkaSource
  */
object StreamingKafkaSource {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "Mark_test"

    val prop = new Properties()

    prop.setProperty("bootstrap.servers","hadoop100:9092")
    prop.setProperty("group.id","con1")

    val kafkaSource = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)

    val data = env.addSource(kafkaSource)

    data.print()

    env.execute("StreamingKafkaSource")


  }

}
