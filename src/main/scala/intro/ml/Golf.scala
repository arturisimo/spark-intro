package intro.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Machine learning. con mecanimo supervisado. A partir de un datos se genera un modelo para tomar una decision
 * 
 * MLlib is Apache Spark's scalable machine learning library. 
 * https://spark.apache.org/mllib/
 * https://spark.apache.org/docs/latest/ml-guide.html
 * 
 * Algorimo para decidir si se juega al gof o no
 * 
 * Después de usar un algoritmo de clasificación de árbol de decisiones de
 * aprendizaje automático contra un conjunto de datos de entrada, se creará un
 * modelo que evaluará cada atributo y progresará a través del "árbol" hasta que
 * se llegue a un nodo de decisión.
 * 
 * hdfs dfs -put golf.csv /loudacre/
 * 
 */
object Golf extends App {
  
    val spark = SparkSession.builder.getOrCreate()
    
    val inputDF = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/loudacre/golf.csv") //CSV
    
    //Crear árbol


    val model = DecisionTree.trainClassifier(data=datos, numClasses=2,  categoricalFeaturesInfo={0: 3})
    
    println(model.toDebugString())
    
    println(model.predict([1.0, 85, 85, True]))
    
    
    spark.stop()
  
}