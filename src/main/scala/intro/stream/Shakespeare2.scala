package intro.stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time

object Shakespeare2 {
  
  def main(args: Array[String])
  {
    def addTuples( x: (Int, Int), y: (Int, Int) ) =
      ( x._1+y._1, x._2+y._2 ) 

    def actualizar( nuevos : Seq[(Int, Int)], estado : Option[(Int, Int)] ) =
    {
      val n = nuevos.fold((0, 0))( addTuples( _ , _ ) )
      Some( addTuples( n, estado.getOrElse( (0, 0) ) ) )
    }

    def imprimirEstadisticas( r : RDD[(Char, (Int, Double))] ) {
      for (elem <- r.collect.sorted )
        elem match {
          case( letra, ( total, media ) ) =>
            println( f"${letra} ${total}%6d ${media}%5.2f" )
        }
    }

    val sconf = new SparkConf().setAppName("Shakespeare02")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc,Seconds(10))
    ssc.checkpoint("shakespeare")

    val logs = ssc.socketTextStream( "localhost", 9999 )
    val estadisticas = logs.
      filter( _.length > 0 ).
      flatMap( _.split("\\W+") ).
      filter( _.length > 0).
      filter( _(0).isLetter ).
      map( w => ( w(0).toLower, ( w.length, 1 ) ) ).
      reduceByKey( addTuples( _ , _ ) ).
      updateStateByKey( actualizar ).
      mapValues( t => (t._2, t._1.toDouble/t._2) ).
      foreachRDD( imprimirEstadisticas( _ ) )

    ssc.start()
    ssc.awaitTermination()
  }
}
