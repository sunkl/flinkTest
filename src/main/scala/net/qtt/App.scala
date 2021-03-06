package net.qtt

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Hello world!
 *
 */
object App  {
  def kafkaConsumerInstance(topic:String): FlinkKafkaConsumer[String] ={
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "flink-group")
    new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props)
  }
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val stream= streamEnv.addSource(kafkaConsumerInstance("test_topic"))
    .flatMap(line=>line.split(","))
        .map(word=>(word,1))
        .keyBy(0)
        .timeWindow(Time.seconds(3))
//    stream.print()
    streamEnv.execute("fink_test")
  }
}
