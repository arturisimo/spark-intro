package intro.stream.window

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
 * Operaciones sobre "Ventanas" countByWindow
 * 
 * calculo acumulativo sobre DStream updateStateByKey
 * 
 * Estas operacions requieren checkpointig (sobre el estado) 
 * 
 * Ejecutar websocket
 * 		python $DEVSH/scripts/streamtest.py gateway 1234 20 $DEVDATA/weblogs/\*
 * 
 * Ejecutar spark
 * 		spark2-submit --class intro.stream.StreamingLogsMB --master yarn --conf spark.default.parallelism=4  target/spark-intro-1.0.jar gateway 1234
 * 
 * 
 * 
 */
object StreamingLogsMB  extends App {

    if (args.length < 2) {
      System.err.println("Usage: stubs.StreamingLogsMB <hostname> <port>")
      System.exit(1)
    } 
 
    // get hostname and port of data source from application arguments
    val hostname = args(0)
    val port = args(1).toInt

    // Create a Spark Context
    val sc = new SparkContext()

    // Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))
    
    ssc.checkpoint("logcheckpt")
    
    // Create a DStream of log data from the server and port specified   
    val logs = ssc.socketTextStream(hostname, port)
    
    
    //Cuentas el numero de paginas en una ventana de 5s cada 2s 
    val numLogs = logs.countByWindow(Seconds(5), Seconds(2)).print()
    
     val reqcountsByWindow = logs.map(line => (line.split(' ')(2),1))
                                .reduceByKeyAndWindow((v1: Int, v2: Int) => v1+v2, Seconds(5), Seconds(2))
    
    
 
    
    //calculo acumulativo de un DStream a partir de estado previos actualiza con los nuevos batch
    //se le pasa como paramentro una funcion update de estados
    val totalUserreqs = reqcountsByWindow.updateStateByKey(updateCount)

    val topUserreqsByWindow = totalUserreqs.transform(rdd => rdd.sortBy(_._2, false))
    
    totalUserreqs.print                                
                                
    
    ssc.start()
    ssc.awaitTermination()
    
    
   /**
    * función que toma una secuencia de hits y suma 
    * (trait Lista implementa Seq) 
    */
   def updateCount = (newCounts: Seq[Int], state: Option[Int] ) => {
      //val newCount = newCounts.foldLeft(0)(_+_)
      val newCount = newCounts.sum
      val previousCount = state.getOrElse(0)
      Some(newCount + previousCount)
   }
    
  
}