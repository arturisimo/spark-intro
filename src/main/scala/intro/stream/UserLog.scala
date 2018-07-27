package intro.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds

/**
 * Flujo de datos: generación continua con RDD
 * DStream—RDDs DStream (Discretized Stream) divide el flujo de datos en batches de n segundos
 * Procesa cada batch en un RDD. Las operaciones y transformaciones son las del RDD
 * Devuelve de la operaciones de cada RDD
 * 
 * DOC: https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * 
 * Genera un DStream a partir de un flujo de datos de un websocket 
 */ 
object USerLog extends App {
  
    var socketHostname = "192.123.12.1"
    var socketPort = 80
  
    val spark = SparkSession.builder.getOrCreate()
    
    //genera RDD cada 2 seg
    val ssc = new StreamingContext(new SparkContext(),Seconds(2))
    
    //se conecta a un socket
    val mystream = ssc.socketTextStream(socketHostname, socketPort)
    //datastream con usuario-numero de peticiones
    val userreqs = mystream.map(line => (line.split(' ')(2),1)).reduceByKey((x,y) => x+y)
    
    //imprime los 10 primeros de cada RDD
    userreqs.print()
 
}