package intro.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

/**
 * Grados de separacion de heroes de marvel
 * 
 * Obtener el GS de un heroe con cada uno de los heroes
 * 
 * Algoritmo
 *    Algoritmo de grafos: breadth-first search
 * 
 * COLORES:
 *  blanco - no se ha visitado
 *  gris  - analisis 
 *  negro - analisis terminado
 * 
 * Input 
 *    Marvel-graph.txt
 *    Marvel-graph-corto.txt
 *    
 *  hdfs dfs -put /home/training/training_materials/devsh/exercises/marvel/Marvel-corto.txt /loudacre/
 *     
 *     
 *     
 */
object Marvels extends App {
     
   if (args.length < 2) {
      System.err.println("Usage: rdd.Marvels fichero de relaciones de marvels")
      System.exit(1)
   }
  
   val spark = SparkSession.builder.getOrCreate()
   
   //val marvelRelationsInput = spark.sparkContext.textFile("/loudacre/Marvel-corto.txt")
    /**
    * (vertice, (conexiones, distancia, color))
    */
   def getRDDInicial(args: Array[String]) : RDD[String] = {
     val marvelRelationsInput = spark.sparkContext.textFile(args(0))
     
     if( marvelRelationsInput.map(line => line.split(" ")).filter(fields => fields(0) == args(1)).count == 0 ) {
       System.err.println("Usage: no esta este heroe")
       System.exit(1)
     }
     
     marvelRelationsInput.map(line => (line.head, line.tail.toList, (if (args(1) == line.head) 0 else 9999), (if (args(1) == line.head) "GRIS" else "BLANCO")))
     marvelRelationsInput
   }
   
   //(1, ([2, 3], 9999, 'BLANCO'))
   val gsOrigen = getRDDInicial(args)
   
    for (gs <- gsOrigen)
    println(gs)
    
   
   
   
   
  
}