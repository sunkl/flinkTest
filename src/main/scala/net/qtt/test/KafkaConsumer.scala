package net.qtt.test

import java.util.Properties

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.consumer.ConsumerRecord
    import org.apache.kafka.clients.consumer.ConsumerRecords
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import java.time.Duration
    import java.util
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "test")
    props.setProperty("enable.auto.commit", "true")
    props.setProperty("auto.commit.interval.ms", "1000")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test_topic"))
    while ( true ) {
      val records = consumer.poll(Duration.ofMillis(100))
      import scala.collection.JavaConversions._
      for (record <- records) {
          println(s"offset:${record.offset},key:${record.key}, value:${record.value}")
      }
    }
  }
}
