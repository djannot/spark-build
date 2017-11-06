import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random
import org.apache.spark.streaming.receiver._

import scala.annotation.tailrec
import scala.util.Random


object KafkaProducer {
  class DummySource(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

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
        store("I am a dummy source " + Random.nextInt(10))
        Thread.sleep((1000.toDouble / ratePerSec).toInt)
      }
    }
  }



  @tailrec
  def doRandomString(n: Int, charSet:Seq[Char], list: List[Char]): List[Char] = {
    val rndPosition = Random.nextInt(charSet.length)
    val rndChar = charSet(rndPosition)
    if (n == 1) rndChar :: list
    else doRandomString(n - 1, charSet, rndChar :: list)
  }

  def randomString(n: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    doRandomString(n, chars, Nil).mkString
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("USAGE: <brokerlist> <topic>")
    }
    val Array(brokers, topic) = args
    println(s"Got brokers $brokers, and producing to topic $topic")
    val conf = new SparkConf().setAppName("Spark->Kafka Producer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val stream = ssc.receiverStream(new DummySource(1))
    val wordStream = stream.flatMap { l => l.split(" ")}
    val wordCountStream = wordStream.map { w => (w, 1) }.reduceByKey(_ + _)
    wordCountStream.foreachRDD { rdd => rdd.toDF("word", "count").registerTempTable("batch_word_count") }

    wordCountStream.foreachRDD { rdd =>
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
