package intro.kafka

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

/**
 * Programa que imprime el numero de peticiones por usuario usando como fuente Kafka
 * 
 * 2 argumentos:
 *    Kafka topic
 *    Lista de kafka brokers 
 * 
 * Nuevo kafka topic weblogs
 *    kafka-topics --create --zookeeper master-1:2181 --replication-factor 1 --partitions 2 --topic weblogs
 * 
 * lista de kafka topics
 *    kafka-topics --list --zookeeper master-1:2181
 *    
 * script para generar weblogs
 *    $DEVSH/scripts/streamtest-kafka.sh weblogs master-1:9092 20 $DEVDATA/weblogs/*   
 *    
 * spark2-submit --class intro.kafka.StreamingRequestCount target/spark-intro-1.0.jar weblogs master-1:9092
 * **/
 */
object StreamingRequestCount extends App {
  
   if (args.length < 2) {
      System.err.println("Usage: intro.kakfa.StreamingLogsKafka <topic> <brokerlist>")
      System.exit(1)
    }  
    val topic = args(0)
    val brokerlist = args(1)
  
  
  val ssc = new StreamingContext(new SparkContext(), Seconds(2))
  
  //lista de brokers disponibles
  val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder] (ssc, Map("metadata.broker.list"-> brokerlist), Set(topic))
  
  val logs = kafkaStream.map(pair => pair._2)
  val userreqs = logs.map(line => (line.split(' ')(2),1)).reduceByKey((x,y) => x+y)
  
  userreqs.print
  
  ssc.start
  
  ssc.awaitTermination

}