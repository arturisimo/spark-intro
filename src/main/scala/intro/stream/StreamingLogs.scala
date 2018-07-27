package intro.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

/**
 * Ejecutar websocket
 * 		python $DEVSH/scripts/streamtest.py gateway 1234 20 $DEVDATA/weblogs/\*
 * 
 * Ejecutar spark
 * 		spark2-submit --class intro.stream.StreamingLogs --master yarn --conf spark.default.parallelism=4  target/spark-intro-1.0.jar gateway 1234
 * 
 */
object StreamingLogs extends App {
  
  
    val spark = SparkSession.builder.getOrCreate()
  
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
  
    val ssc = new StreamingContext(sc, Seconds(1))
    
    if (args.length < 2) {
      System.err.println("Usage: stream.StreamingLogs hostname port")
      System.exit(1)
   }
 
   val hostname = args(0)
   val port = args(1).toInt
    
   val mystream = ssc.socketTextStream(hostname, port)
    
   val logsStreaming = mystream.filter(line => line.contains("KBDOC"))
    
    logsStreaming.print(5)
    
   //For each RDD in the filtered DStream, display the number of items—that is, the
   //number of requests for KB articles.
   
   // Print out the count of each batch RDD in the stream
    logsStreaming.foreachRDD(rdd => println("Number of KB requests: " + rdd.count()))
   
    logsStreaming.saveAsTextFiles("/loudacre/streamlog/kblogs")
   
   
    ssc.start()
    
    ssc.awaitTermination()
  
}