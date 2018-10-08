package domain

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringSerializer

class CustomKafkaProducer() {
  val props = new Properties
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all") // blocking - will await confirm of received messages
  props.put("enable.idempotence", true.asInstanceOf[java.lang.Boolean])
  props.put("batch.size", 128.asInstanceOf[java.lang.Integer]) // max num of records to be batched together
  props.put("buffer.memory", 33554432.asInstanceOf[java.lang.Integer]) // memory buffer for batches
  props.put("linger.ms", 100.asInstanceOf[java.lang.Integer]) // records will be sent if buffer is unreached during linger.ms
  // If records are sent faster than they can be transmitted to the server then the buffer space will be exhausted.
  // When the buffer space is exhausted additional send calls will block.
  // The threshold for time to block is determined by max.block.ms after which it throws a TimeoutException.
  props.put("max.block.ms", 100.asInstanceOf[java.lang.Integer])
  props.put("client.id", "Producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val avroProducer = new KafkaProducer[Integer, Array[Byte]](props)
  val stringProducer = new KafkaProducer[Integer, String](props)

  def sendAvro(topic: String, message: Array[Byte]) = {
    avroProducer.send(new ProducerRecord[Integer, Array[Byte]](topic, message))
  }

  def sendString(topic: String, message: String) = {
    stringProducer.send(new ProducerRecord[Integer, String](topic, message))
  }

  def exit() {
    println("Closing Kafka producer...")
    avroProducer.flush()
    avroProducer.close()
    stringProducer.flush()
    stringProducer.close()
  }
}
