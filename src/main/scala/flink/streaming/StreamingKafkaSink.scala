package flink.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object StreamingKafkaSink {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = List("123","44","5","66","7")

    val source = env.fromCollection(data)

    val topic = "Mark_test"

    val prop = new Properties()

    prop.setProperty("bootstrap.servers","hadoop100:9092")

    // 使用支持仅一次语义
    val produce = new FlinkKafkaProducer011[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    source.addSink(produce)

    env.execute("StreamingKafkaSink")

  }

}
