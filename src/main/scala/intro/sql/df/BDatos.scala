package intro.sql.df

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext

/**
 * Crea un DataFrame a partir de una tabla llamada “clientes” almacenada en una base de datos MySQL
 * 
 * 
 * spark2-submit --class intro.df.BD --master yarn --conf spark.default.parallelism=4  target/spark-intro-1.0.jar
 * 
 */
object BDatos extends App {
  
  
  val spark = SparkSession.builder.getOrCreate()
  
  val sc = spark.sparkContext.setLogLevel("WARN")
  
  val url = "jdbc:mysql://localhost:3306/loudacre"
  
  val df = spark.read.format("jdbc").option("url", url).option("dbtable", "device").option("user", "root").option("password", "cloudera").load()
 
// Muestra el esquema del DataFrame&lt;
  df.printSchema()
   
  // Cuenta a los clientes por su edad
  val countsByName = df.groupBy("device_name").count()
  countsByName.show()
  
  
}