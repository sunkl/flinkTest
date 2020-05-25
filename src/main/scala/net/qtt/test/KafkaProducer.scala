package net.qtt.test

import java.util.Properties

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.producer.KafkaProducer
    import org.apache.kafka.clients.producer.Producer
    import org.apache.kafka.clients.producer.ProducerRecord
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val sourceWorld = Array("hello","flink","qu","tou","tiao")
    for (i <- 0 until 100) {
      Thread.sleep(1000*2)
      producer.send(new ProducerRecord[String, String]("test_topic", Integer.toString(i),sourceWorld(i % sourceWorld.length)))
      println(sourceWorld(i % sourceWorld.length))
    }
  }
}
