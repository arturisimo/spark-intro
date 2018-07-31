package intro.sql.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 

/**
 * DataFrames pueden ser construidos a partir de diversas fuentes de datos:
 * 
 * ─ ficheros planos: CSV ─ JSON ─ Plain text - Semi-estrucurados su estructura se puede inferir de la cabecera o de los datos 
 * ─ Binarios : Apache Parquet ─ Apache ORC - Datos Estructurados
 * ─ Tablas: Hive metastore ─ JDBC - Cassandra
 * - RDD existente
 * 
 * Se referencian con URI absolutas o relativas:
 *  myfile.json
 *  hdfs://nnhost/loudacre/myfile.json
 *  file:/home/training/myfile.json
 * 	
*/
object Empresa extends App {
  
      val spark = SparkSession.builder.getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
  
      //import a HDFS empresa.csv
      //$ hdfs dfs -put empresa.csv /loudacre/empresa.csv

      // df empresa.csv inferSchema infiere el tipo de cada campo. Header true dice que tiene una cabecera
      val empresaDF = spark.read.format("csv").option("inferSchema", "true")
                                              .option("header", "true")
                                              .load("/loudacre/empresa.csv")

      /* un dataframe tiene un esquema
    scala> empresaDF.printSchema
    root
     |-- department: string (nullable = true)
     |-- id: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- age: integer (nullable = true)
     |-- salary: double (nullable = true)
*/


      val calculoDF =  empresaDF.groupBy(empresaDF("department").as("departamento"))
                            .agg(round(sum(empresaDF("salary")),2).as("salarioTotal"), 
                                round(avg(empresaDF("salary")),2).as("media"), 
                                round(variance(empresaDF("salary")),2).as("varianza"),
                                round(stddev_samp(empresaDF("salary")),2).as("desviacion"),  
                                max(empresaDF("age")).as("mayorEdad"), 
                                min(empresaDF("age")).as("menorEdad"))

      /*
      scala> calculoDF.show
      +------------+------------+-------+---------+----------+---------+---------+
      |departamento|salarioTotal|  media| varianza|desviacion|mayorEdad|menorEdad|
      +------------+------------+-------+---------+----------+---------+---------+
      |    finanzas|      5851.2| 1950.4|232125.21|    481.79|       41|       34|
      |   marketing|     8250.45|2062.61|461987.97|     679.7|       50|       20|
      +------------+------------+-------+---------+----------+---------+---------+
        */

      spark.stop
      
      
}