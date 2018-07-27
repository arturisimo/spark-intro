package intro.stream.window

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions


/**
 * operaciones Window sobre un DSTream: reduceByKeyAndWindow, countByWindow
 * 
 * python $DEVSH/scripts/streamtest.py gateway 1234 20 $DEVDATA/weblogs/\*
 * 
 * spark2-submit --class intro.stream.Windows target/spark-intro-1.0.jar gateway 1234
 * 
 */
object Windows  extends App {

    if (args.length < 2) {
      System.err.println("Usage: stubs.Window <hostname> <port>")
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
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create a DStream of log data from the server and port specified   
    val logs = ssc.socketTextStream(hostname, port)
    
    //Cada 30s cuenta las peticiones realizadas en los ultimos 5min 
    val reqcountsByWindow = logs.map(line => (line.split(' ')(2),1)).    
                                .reduceByKeyAndWindow((v1: Int, v2: Int) => v1+v2, Minutes(5), Seconds(30))
    
    //imprimimos las que tiene mas peticiones
    //val topreqsByWindow = reqcountsByWindow.map(pair => pair.swap).transform(rdd => rdd.sortByKey(false))
    //val topreqsByWindow = reqcountsByWindow.foreachRDD(rdd=>rdd.sortBy(t => t._2 > t._2, false))
    val topreqsByWindow = reqcountsByWindow.transform(rdd => rdd.sortBy(_._2, false))
    
    topreqsByWindow.map(pair => pair.swap).print



    ssc.start()
    ssc.awaitTermination()
  
}