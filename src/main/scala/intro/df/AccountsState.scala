package intro.df

import org.apache.spark.sql.SparkSession

/**
 * DataFrames - org.apache.spark.sql.DataFrame
 * Objeto con datos estructurados como una tabla: coleccion inmutable de objetos de tipo Row (parecido a una tupla)
 * Internamente genera un esquema 
 * 
 * Es una evolucion de los RDD y requieren un Spark Session
 * Spark Session
 * Es el intermediario entre spark y el programa (En el codigo al principio hay que llamar a SparkSession, en la shell se crea este objeto por defecto)
 * La clase SparkSession provee funciones y atributos para acceder a las funcionalidades de Spark
 */
object AccountsState extends App {
    
    if (args.length < 1) {
      System.err.println("Usage: solution.AccountsByState <state-code>")
      System.exit(1)
    }
 
    val stateCode = args(0)
    
   
    val spark = SparkSession.builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    //carga una tabla HIVE
    val accountsDF = spark.read.table("accounts")
    val stateAccountsDF = accountsDF.where(accountsDF("state") === stateCode)

    //escritura
    stateAccountsDF.write.mode("overwrite").save("/loudacre/accounts_by_state/"+stateCode)

    spark.stop()
  
}