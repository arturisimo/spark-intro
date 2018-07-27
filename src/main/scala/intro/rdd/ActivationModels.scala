package intro.rdd


import scala.xml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * RDD Resilient Distributed Dataset
 * Representa una coleccion distribuidad de elemwntos (de cualquier tipo). No tiene estructura
 * 
 * generacion a partir de un objeto en memoria val myRDD = sc.parallelize(myData)
 * 
 * RDDs se generan a partir de datos:
 * 
 * ─ Text files and other data file formats
 * ─ Data in other RDDs
 * ─ Data in memory
 * ─ DataFrames and Datasets
 * 
 * RDDs contine datos sin esctrucuta no tiene un esquema asociado
 * 
 * Operaciones RDD 
 *  ─ Transformaciones generar un RDD a partir de otro 
 *  ─ Actiones devuelve un valor a partir del RDDs return a value from an RDD
 *  
 *  Una query RDD consiste en una secuencia de transformaciones ejecutadas por una accion:
 *  Las RDD se ejecutan de forma LAZY cuando las invoca una acción
 *  
 *	
 *	PARTICIONES
 *	
 *	Ver particiones de un RDD: myRDD.getNumPartitions
 *	
 *		Task   unidad de trabajo que se ejecuta
 *		Stage  Conjunto de tareas asociados a un job que se pueden ejecutar en paralelo
 *		Jobs — Secuncia de stages como resultado de una accion
 *	  Application conjunto de jobs manejador por un único driver
 *	
 *		Las particiones de Spark de dividen en varios ejecutores en la apliacion
 *			- Executors execute query tasks that process the data in their partitions
 *			- Narrow operations like map and filter are pipelined within a single stage
 *	─ Wide operations like groupByKey and join shuffle and repartition data
 *	between stages
 *	
 *	Los jons se ejecutan de acuerdo a un plan de ejccucion
 *	─ Core Spark creates RDD execution plans based on RDD lineages
 *	─ Catalyst builds optimized query execution plans
 *	
 *	  En HUE hay un Job browser
 * 
 */
object ActivationModels extends App {
    
  
   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
  
    //generacion del RDD
    val actRDD = sc.wholeTextFiles("/loudacre/activations")
    
    
    //val accModelRDD = actRDD.map(t => getActivations(t._2)).flatMap(x => x).map(x => getAccount(x) + ":" + getModel(x))
    val accModelRDD = actRDD.map{case(url, xml) => getActivations(xml)}.flatMap(x => x).map(x => getAccount(x) + ":" + getModel(x))
    
    //se guarda en varios ficheros tantos como particiones
    accModelRDD.saveAsTextFile("/loudacre/accountmodels")
    
      // Given a string containing XML, parse the string, and 
    // return an iterator of activation XML records (Nodes) contained in the string
    def getActivations(xmlstring: String): Iterator[Node] = {
        val nodes = XML.loadString(xmlstring) \\ "activation"
        nodes.toIterator
    }
    
    // Given an activation record (XML Node), return the model name
    def getModel(activation: Node): String = {
       (activation \ "model").text
    }
    
    // Given an activation record (XML Node), return the account number
    def getAccount(activation: Node): String = {
       (activation \ "account-number").text
    }

    spark.stop()
}