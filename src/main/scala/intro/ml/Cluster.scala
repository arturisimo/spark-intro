package intro.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.math.pow

/**
 * Encontrar cluster de puntos latitud - longitud
 * 
 * Algoritmo
 *    Se generan K centros del cluster random
 * 
 *    Cada punto pertenece a un cluster (el que le sea mas cercano) 
 *    Se hace una media arimetica para obtener el nuevo centro
 * 
 *    Se vuelve a hacer todo el proceso con los nuevos centros (si cambian, si no no se intera mas) 
 * 
 * 
 * Input 
 *    Los puntos se obtiene de los campos latitud y longitud de devicestatus_etl 
 *    hdfs dfs -put devicestatus_etl /loudacre/ 
 * 
 * Ejecutar
 *    $ /home/training/workspace/spark-intro mvn clean package
 *    $ spark2-submit --class intro.rdd.Cluster --master yarn --conf spark.default.parallelism=4  target/spark-intro-1.0.jar k-points
 *    
 * 
 * PERSISTENCIA o CACHEO
 * Se puede persistir los DataFrame, Dataset, or RDD
 * Se almacena en memoria o disco solo 
 * Se consigue mejorar el rendimiento
 * Se usa si usamos una query varias veces
 * 
 * Tables and views can be persisted in memory using CACHE TABLE
 * spark.sql("CACHE TABLE people")
 */

object Cluster extends App {
    
    /** This will be used to decide when the k-means calculation is done—when the amount the locations of the
    means changes between iterations is less than convergeDist. A “perfect”
    solution would be 0; this number represents a “good enough” solution. For this
   exercise, use a value of 0.1.*/
   val convergeDist = 0.1D
  
   //val random = 33
  
   if (args.length < 1) {
      System.err.println("Usage: rdd.Cluster K-points")
      System.exit(1)
   }
 
   val numCentroides = args(0).toInt
  
  
  case class Point(lat: Double, long: Double)
  
  val spark = SparkSession.builder.getOrCreate()
  val sc  = spark.sparkContext
  
  val devicestatusETL = sc.textFile("/loudacre/devicestatus_etl")
  
  val points = devicestatusETL.map(line => line.split(","))
                              .map(fields => Point(fields(3).toDouble, fields(4).toDouble))
                              .filter(point => point.lat != 0.0 && point.long != 0.0 )
                              .persist()
  
  //Array Random de puntos de longitud K numCentroides
  val centroides = points.takeSample(false, numCentroides)
  
  val centros = calculateCentros(points, centroides)
  
  //se imprime los centros calculados
  for (centro <- centros)
    println(centro)
  
  /**
   * calculo del centro de los Cluster
   */
  def calculateCentros(points: RDD[Point], centroides: Array[Point]) : Array[Point] = {
      
    // hacemos un reduce por el indice -obtenido del metodo closestPoint-  y calulamos la suma de los puntos
    // hacemos un reduceByKey que es mas eficiente que el groupByKey
    val groupPoints = points.map(point => (closestPoint(point, centroides), (point,1)))
                              .reduceByKey{ (point1, point2) => (addPoints(point1._1, point2._1), point1._2 + point2._2)}
  
    // calculamos los centroides
    val centroidesCal = groupPoints.map{case (index, total) => (index, Point( (total._1.lat/ total._2), (total._1.long / total._2)))} 
    
    // hay que recalcular si alguno de los elementos tiene una distancia mayor a la definida
    val recalculate = centroidesCal.filter{ case (idx,centroideCal) => distanceSquared( centroides(idx), centroideCal) > convergeDist }.count() > 0 
      
    if (recalculate)
        calculateCentros(points, centroidesCal.values.collect())
    else
        centroidesCal.values.collect()
  }  
    
  
  /**
   * Dado un punto devuelve el indice (de un array de puntos) del mas cercano
   */
  def closestPoint ( punto: Point, centroides: Array[Point]): Int = {
    var index = 0
    var closestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centroides.length) {
      val dist = distanceSquared(punto, centroides(i))
      if (dist < closest) {
        closest = dist
        closestIndex = i
      }
    }
    closestIndex   
  }
  
  /**
   * Devuelve un punto que es la suma de ambos puntos
   */
  def addPoints ( point1: Point, point2: Point) = Point(point1.lat + point2.lat, point1.long + point2.long)
 
  /**
   * Calcular la distancia entre dos puntos  (teorema de pitagoras)  
   */
  def distanceSquared ( point1: Point, point2: Point) = pow(point1.lat - point2.lat,2) + pow(point1.long - point2.long,2)
  
  
  spark.stop()
  
}