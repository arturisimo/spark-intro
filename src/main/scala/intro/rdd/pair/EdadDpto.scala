package intro.rdd.pair

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * obtener la media de edad por deparatamento PAIR-RDD
 */
object EdadDpto extends App {
    
  
   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
  
      //RDD origen de datos
    val deRDD = sc.textFile("/loudacre/departamento_edad")

    def isAllDigits(s: String) : Boolean = s.filter(ch => ch.isDigit).size > 0
   
    val resultadoDD = deRDD.map(_.split(","))
						.filter{case(Array(dep, edad)) => isAllDigits(edad) } //filtra cabecera
						.map{case(Array(dep, edad)) => (dep, edad.toInt)}  //pair RDD dpto-edad


    resultadoDD.mapValues(_-> 1) //par RDD
		   .reduceByKey{case ( (e1,c1), (e2,c2) ) => (e1+e2, c1+c2)} //se agrupa por departamento y se suma 
		   .mapValues(t => t._1/t._2.toDouble ) //media de la edad
		   
		spark.stop()
}