import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver._

import scala.util.Random

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("USAGE: <brokerlist> <topic>")
    }

    val Array(brokers, topic) = args

    val conf = new SparkConf().setAppName("Kafka->Spark Validator Consumer")
    val ssc = new StreamingContext(conf, Seconds(2))
    //val props = DefaultProps.getDefaultProps(brokers)

    println(s"Using brokers $brokers and topic $topic")

    val props = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), props))

    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.map(w => w._2)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}


object KafkaProducer {
  class SmartySource(words: Array[String], sentenceLength: Int, ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

    def onStart() {
      // Start the thread that receives data over a connection
      new Thread("Dummy Source") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private def receive() {
      while(!isStopped()) {
        // could do something where you wait for the sentence length to get so long
        // and you add words with their frequency probability
        store(Random.shuffle(words.toList).take(sentenceLength).mkString(" "))
        Thread.sleep((1000.toDouble / ratePerSec).toInt)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new IllegalArgumentException("USAGE: <brokerlist> <file> <topic>")
    }
    val Array(brokers, infile, topic) = args
    println(s"Got brokers $brokers, and producing to topic $topic")
    val conf = new SparkConf().setAppName("Spark->Kafka Producer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val words = sc.textFile(infile).flatMap(l => l.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(t => t._1)
    println(s"Got ${words.count} unique words")
    val stream = ssc.receiverStream(new SmartySource(words.collect, 4, 1))
    stream.foreachRDD { rdd =>
      println(s"Number of events: ${rdd.count()}")
      rdd.foreachPartition { p =>
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        p.foreach { r =>
          val d = r.toString()
          val msg = new ProducerRecord[String, String](topic, null, d)
          producer.send(msg)
        }
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
