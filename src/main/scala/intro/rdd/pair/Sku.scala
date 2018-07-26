package intro.rdd.pair


import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


/**
 *   Pares RDDs son un tipo especial de RDD
 *   ─ cada elemento es un par clave-valor (clave no unico)
 *   ─ la clave y el valor son de cualquier tipo
 * 
 */
object Sku extends App {

   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
  
   //val usersRDD = sc.textFile("userlist.tsv").map(line => line.split('\t').map(fields => (fields(0),fields(1)))

      val myData = List("00001 sku010:sku933:sku022","00002 sku912:sku331","00003 sku888:sku022:sku010:sku594","00004 sku411")
    val myRDD = sc.parallelize(myData)

    // flatMapValues
     val myRDD2 = myRDD.map(_.split(" ")).map(x => (x(0), x(1))).flatMapValues(skus => skus.split(':'))
    //groupByKey + sortByKey . el groupBy agrupa en un objeto iterable CompactBuffer >>  (00001,CompactBuffer(sku010, sku933, sku022))
    val myRDD3 = myRDD2.groupByKey.sortByKey()
    //mapValues
    val myRDD4 = myRDD3.mapValues(_.mkString(":"))
    //por ultimos lo contatenos:
    myRDD4.map{case(id, sku) => s"$id $sku"}.foreach(println)
    
    spark.stop()
  
}