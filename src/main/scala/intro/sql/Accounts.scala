package intro.sql

import org.apache.spark.sql.SparkSession


/**
 * Spark SQL se utiliza datos estructurados mediante DataFrames.
 * La API de Spark SQL permite la conexión a las fuentes de origen, obteniendo así los datos y pasando a ser gestionados en memoria mediante Spark.
 */
object Accounts extends App {
  
    
  val spark = SparkSession.builder.getOrCreate()
  
      spark.catalog.listTables.show
    
    spark.catalog.listColumns("accounts").show
    
    val accountsDF = spark.read.table("accounts")
    accountsDF.printSchema
    
    val firstLastDF = spark.sql("SELECT first_name, last_name FROM accounts")
    firstLastDF.printSchema
    firstLastDF.show(5)
    
    val firstLastDF2 = accountsDF.select("first_name","last_name")
    firstLastDF2.printSchema
    firstLastDF2.show(5)
    
    val accountDeviceDF = spark.read.option("header","true").option("inferSchema","true").csv("/loudacre/accountdevice")
    
    accountDeviceDF.createOrReplaceTempView("account_dev")
    
    spark.catalog.listTables.show
    
    spark.sql("SELECT * FROM account_dev LIMIT 5").show
    
    val nameDevDF = spark.sql("SELECT acct_num, first_name, last_name, account_device_id FROM accounts JOIN account_dev ON acct_num = account_id")
    nameDevDF.show
    
    nameDevDF.write.option("path","/loudacre/name_dev").saveAsTable("name_dev")
    
    spark.catalog.listTables.show
    spark.sql("DESCRIBE name_dev").show
  
    spark.stop
  
  
}