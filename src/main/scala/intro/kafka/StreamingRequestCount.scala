package intro.kafka

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object StreamingRequestCount extends App {
  
  val ssc = new StreamingContext(new SparkContext(), Seconds(2))
  
  //lista de brokers disponibles
  val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder] (ssc, Map("metadata.broker.list"->"broker1:port,broker2:port"), Set("mytopic"))
  
  val logs = kafkaStream.map(pair => pair._2)
  val userreqs = logs.map(line => (line.split(' ')(2),1)).reduceByKey((x,y) => x+y)
  
  userreqs.print
  
  ssc.start
  
  ssc.awaitTermination

}