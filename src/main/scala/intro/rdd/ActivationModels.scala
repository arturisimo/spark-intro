package intro.rdd

import scala.xml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 
 * Tranformación de un RDD procedente de un XML utilizando una libreria (scala.xml) para parsear el XML 
 * 
 * Subir ficheros a HDFS
 *    $ hdfs -put activations /loudacre/
 * 
 * Ejecucion de la aplicacion:
 *    $ spark2-submit --class intro.rdd.ActivationModels target/spark-intro-1.0.jar
 */
object ActivationModels extends App {
    
   val spark = SparkSession.builder.getOrCreate()
  
   val sc = spark.sparkContext
   sc.setLogLevel("ERROR")
  
   //generacion del RDD
   val actRDD = sc.wholeTextFiles("/loudacre/activations")
    
    
   //val accModelRDD = actRDD.map(t => getActivations(t._2)).flatMap(x => x).map(x => getAccount(x) + ":" + getModel(x))
   val accModelRDD = actRDD.map{case(url, xml) => getActivations(xml)}
                             .flatMap(x => x)
                             .map(x => getAccount(x) + ":" + getModel(x))
    
   //se guarda en varios ficheros tantos como particiones
   accModelRDD.saveAsTextFile("/loudacre/accountmodels")
    
    /**
     * Given a string containing XML, parse the string, and 
     * return an iterator of activation XML records (Nodes) contained in the string
     */
    def getActivations(xmlstring: String): Iterator[Node] = {
        val nodes = XML.loadString(xmlstring) \\ "activation"
        nodes.toIterator
    }
    
    /**
     * Given an activation record (XML Node), return the model name
     */
    def getModel(activation: Node): String = {
       (activation \ "model").text
    }
    
    /**
     * Given an activation record (XML Node), return the account number
     */
    def getAccount(activation: Node): String = {
       (activation \ "account-number").text
    }

    spark.stop()
}