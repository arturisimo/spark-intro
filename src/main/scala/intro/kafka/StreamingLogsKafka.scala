package intro.stream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

/**
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
 * spark2-submit --class intro.stream.StreamingLogsKafka target/spark-intro-1.0.jar weblogs master-1:9092
 * **/
 */
object StreamingLogsKafka extends App {
  
    if (args.length < 2) {
      System.err.println("Usage: solution.StreamingLogsKafka <topic> <brokerlist>")
      System.exit(1)
    }  
    val topic = args(0)
    val brokerlist = args(1)
    
    val sc = new SparkContext()
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(1))

    // DStream de logs del topoc de Kakfa  
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, Map("metadata.broker.list"->brokerlist), Set(topic))

    // The weblog data is in the form (key, value), map to just the value
    val logStream = kafkaStream.map(pair => pair._2)

    // To test, print the first few lines of each batch of messages to confirm receipt
    logStream.print()
        
    // se imprime elñ numero de logs de cada batch
    logStream.foreachRDD(rdd => println("Number of requests: " + rdd.count()))

    // se guardan los logs en HDFS
    logStream.saveAsTextFiles("/loudacre/streamlog/kafkalogs")
    
    ssc.start()
    ssc.awaitTermination()
    
    
}