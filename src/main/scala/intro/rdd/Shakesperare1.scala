package intro.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * hdfs dfs -put Shakespeare.txt /loudacre/
 * 
 * spark2-submit --class intro.df.Shakesperare1 target/spark-intro-1.0.jar
 */
object Shakesperare1 extends App {
    
    def addTuples( x: (Int, Int), y: (Int, Int) ) =
      ( x._1+y._1, x._2+y._2 ) 

    val sconf = new SparkConf().setAppName("Shakespeare01")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    val estadisticas = sc.textFile("/loudacre/Shakespeare.txt").
      filter( _.length > 0 ).
      flatMap( _.split("\\W+") ).
      filter( _.length > 0).
      filter( _(0).isLetter ).
      map( w => ( w(0).toLower, ( w.length, 1 ) ) ).
      reduceByKey( addTuples( _ , _ ) ).
      mapValues( t => (t._2, t._1.toDouble/t._2) ).
      collect()

    println()
    for (t <- estadisticas.sorted )
      t match {
        case( letra, ( total, media ) ) =>
          println( f"${letra} ${total}%6d ${media}%5.2f" )
      }

    sc.stop()
}
