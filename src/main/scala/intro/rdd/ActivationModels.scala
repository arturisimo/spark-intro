package intro.rdd


import scala.xml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * RDD Resilient Distributed Dataset
 * Representa una coleccion distribuidad de elemwntos (de cualquier tipo). No tiene estructura
 * 
 * 
 * generacion a partir de un objeto en memoria val myRDD = sc.parallelize(myData)
 * 
 * RDDs se generan a partir de datos:
 * 
 * ─ Text files and other data file formats
─ Data in other RDDs
─ Data in memory
─ DataFrames and Datasets
▪ RDDs contain unstructured data
─ No associated schema like DataFrames and Datasets
▪ RDD Operations
─ Transformations create a new RDD based on an existing one
─ Actions return a value from an RDD

▪ An RDD query consists of a sequence of one or more transformations
completed by an action
▪ RDD queries are executed lazily
─ When the action is called
▪ RDD queries are executed differently than DataFrame and Dataset queries
─ DataFrames and Datasets scan their sources to determine the schema
eagerly (when created)
─ RDDs do not have schemas and do not scan their sources before loading

PARTICIONES

	Jobs —a set of tasks executed as a result of an action
  Stage —a set of tasks in a job that can be executed in parallel
	Task—an individual unit of work sent to one executor
	Application—the set of jobs managed by a single driver

	Las particiones de Spark de dividen en varios ejecutores en la apliacion
		- Executors execute query tasks that process the data in their partitions
		- Narrow operations like map and filter are pipelined within a single stage
─ Wide operations like groupByKey and join shuffle and repartition data
between stages
▪ Jobs consist of a sequence of stages triggered by a single action
▪ Jobs execute according to execution plans
─ Core Spark creates RDD execution plans based on RDD lineages
─ Catalyst builds optimized query execution plans
▪ You can explore how Spark executes queries in the Spark Application UI


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